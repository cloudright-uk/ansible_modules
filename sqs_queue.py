#!/usr/bin/python
# Copyright: Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type


ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}


DOCUMENTATION = """
---
module: sqs_queue
short_description: Creates or deletes AWS SQS queues.
description:
  - Create or delete AWS SQS queues.
  - Update attributes on existing queues.
  - FIFO queues supported
version_added: "2.5"
author:
  - Nathan Webster (@nathanwebsterdotme)
requirements:
  - "boto3 >= 1.44.0"
  - "botocore >= 1.85.0"
options:
  state:
    description:
      - Create or delete the queue
    required: false
    choices: ['present', 'absent']
    default: 'present'
  name:
    description:
      - Name of the queue.
    required: true
  queue_type:
    description:
      - The type of queue to create.  Can only be set at creation time and can't be changed later.  Will append .fifo to name of queue as this is required by AWS.
    required: false
    choices: ['standard', 'fifo']
    default: standard
  default_visibility_timeout:
    description:
      - The default visibility timeout in seconds.
    required: false
    default: null
  message_retention_period:
    description:
      - The message retention period in seconds.
    required: false
    default: null
  maximum_message_size:
    description:
      - The maximum message size in bytes.
    required: false
    default: null
  delivery_delay:
    description:
      - The delivery delay in seconds.
    required: false
    default: null
  receive_message_wait_time:
    description:
      - The receive message wait time in seconds.
    required: false
    default: null
  policy:
    description:
      - The json dict policy to attach to queue
    required: false
    default: null
  redrive_policy:
    description:
      - json dict with the redrive_policy (see example)
    required: false
    default: null
  fifo_content_based_deduplication:
    description:
      - Option to enable / disable Content Based Deduplication.  'queue_type' must be set to 'fifo' to use this option.
    required: False
    choices: ['true', 'false']
    default: false

extends_documentation_fragment:
    - aws
    - ec2
"""

RETURN = '''
default_visibility_timeout:
    description: The default visibility timeout in seconds.
    type: int
    returned: always
    sample: 30
delivery_delay:
    description: The delivery delay in seconds.
    type: int
    returned: always
    sample: 0
maximum_message_size:
    description: The maximum message size in bytes.
    type: int
    returned: always
    sample: 262144
message_retention_period:
    description: The message retention period in seconds.
    type: int
    returned: always
    sample: 345600
name:
    description: Name of the SQS Queue
    type: string
    returned: always
    sample: "queuename-987d2de0"
queue_arn:
    description: The queue's Amazon resource name (ARN).
    type: string
    returned: on successful creation or update of the queue
    sample: 'arn:aws:sqs:us-east-1:199999999999:queuename-987d2de0'
receive_message_wait_time:
    description: The receive message wait time in seconds.
    type: int
    returned: always
    sample: 0
region:
    description: Region that the queue was created within
    type: string
    returned: always
    sample: 'us-east-1'
fifo_content_based_deduplication:
    description: Bool if Content Based Deduplication is enabled for FIFO queues.
    type: string
    returned: always
    sample: 'true'
'''

EXAMPLES = '''
# Create SQS queue with redrive policy
- sqs_queue:
    name: my-queue
    region: ap-southeast-2
    default_visibility_timeout: 120
    message_retention_period: 86400
    maximum_message_size: 1024
    delivery_delay: 30
    receive_message_wait_time: 20
    policy: "{{ json_dict }}"
    redrive_policy:
      maxReceiveCount: 5
      deadLetterTargetArn: arn:aws:sqs:eu-west-1:123456789012:my-dead-queue

# Create a FIFO type queue with Content Deduplication enabled.
- sqs_queue:
    name: my-queue
    queue_type: fifo
    fifo_content_based_deduplication: true
    region: ap-southeast-2
    default_visibility_timeout: 120
    message_retention_period: 86400
    maximum_message_size: 1024
    delivery_delay: 30
    receive_message_wait_time: 20
    policy: "{{ json_dict }}"
    redrive_policy:
      maxReceiveCount: 5
      deadLetterTargetArn: arn:aws:sqs:eu-west-1:123456789012:my-dead-queue

# Delete SQS queue
- sqs_queue:
    name: my-queue
    region: ap-southeast-2
    state: absent
'''

import json
import traceback

try:
  import botocore
  import boto3
  HAS_BOTO3 = True
except ImportError:
  HAS_BOTO3 = False

from ansible.module_utils.aws.core import AnsibleAWSModule
from ansible.module_utils.ec2 import get_aws_connection_info, ec2_argument_spec, boto3_conn, camel_dict_to_snake_dict
from ansible.module_utils.ec2 import AnsibleAWSError, connect_to_aws, ec2_argument_spec, get_aws_connection_info

import pdb

def create_or_update_sqs_queue(connection, module):
    queue_name = module.params.get('name')
    queue_type = module.params.get('queue_type')
    existing_queue = False

    queue_attributes = dict(
        default_visibility_timeout=module.params.get('default_visibility_timeout'),
        message_retention_period=module.params.get('message_retention_period'),
        maximum_message_size=module.params.get('maximum_message_size'),
        delivery_delay=module.params.get('delivery_delay'),
        receive_message_wait_time=module.params.get('receive_message_wait_time'),
        policy=module.params.get('policy'),
        redrive_policy=module.params.get('redrive_policy')
    )

    # If FIFO queue, change name as it must end in .fifo
    # Also add the ContentBasedDeduplication attributes to the queue_attributes dictionary
    if queue_type == 'fifo':
        queue_name = queue_name + '.fifo'
        try:
            queue_attributes.update({'fifo_content_based_deduplication':module.params.get('fifo_content_based_deduplication')})
        except botocore.exceptions.ClientError:
            result['msg'] = 'Failed to create/update sqs queue due to error: ' + traceback.format_exc()
            module.fail_json(**result)

    result = dict(
        region=module.params.get('region'),
        name=queue_name,
    )
    result.update(queue_attributes)

    try:
        # Check to see if the queue already exists
        try:
            queue_url = connection.get_queue_url(QueueName=queue_name)['QueueUrl']
            existing_queue = True
        except:
            pass

        if existing_queue:
            result['changed'] = update_sqs_queue(connection, queue_url, check_mode=module.check_mode, **queue_attributes)
        else:
            # Create new one if it doesn't exist and not in check mode
            if not module.check_mode:
                # Add attributes for a FIFO queue type
                if queue_type == 'fifo':
                    queue_url = connection.create_queue(QueueName=queue_name, Attributes={'FifoQueue':'True'})['QueueUrl']
                    result['changed'] = True
                else:
                    queue_url = connection.create_queue(QueueName=queue_name)['QueueUrl']
                    result['changed'] = True

                update_sqs_queue(connection, queue_url, check_mode=module.check_mode, **queue_attributes)

        if not module.check_mode:
            queue_attributes = connection.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['All'])
            result['queue_arn'] = queue_attributes['Attributes']['QueueArn']
            result['default_visibility_timeout'] = queue_attributes['Attributes']['VisibilityTimeout']
            result['message_retention_period'] = queue_attributes['Attributes']['MessageRetentionPeriod']
            result['maximum_message_size'] = queue_attributes['Attributes']['MaximumMessageSize']
            result['delivery_delay'] = queue_attributes['Attributes']['DelaySeconds']
            result['receive_message_wait_time'] = queue_attributes['Attributes']['ReceiveMessageWaitTimeSeconds']
            if queue_type == 'fifo':
                result['fifo_content_based_deduplication'] = queue_attributes['Attributes']['ContentBasedDeduplication']

    except botocore.exceptions.ClientError:
        result['msg'] = 'Failed to create/update sqs queue due to error: ' + traceback.format_exc()
        module.fail_json(**result)
    else:
        module.exit_json(**result)


def update_sqs_queue(connection,
                     queue_url,
                     check_mode=False,
                     default_visibility_timeout=None,
                     message_retention_period=None,
                     maximum_message_size=None,
                     delivery_delay=None,
                     receive_message_wait_time=None,
                     policy=None,
                     redrive_policy=None,
                     fifo_content_based_deduplication=None):
    changed = False

    changed = set_queue_attribute(connection, queue_url, 'VisibilityTimeout', default_visibility_timeout,
                                  check_mode=check_mode) or changed
    changed = set_queue_attribute(connection, queue_url, 'MessageRetentionPeriod', message_retention_period,
                                  check_mode=check_mode) or changed
    changed = set_queue_attribute(connection, queue_url, 'MaximumMessageSize', maximum_message_size,
                                  check_mode=check_mode) or changed
    changed = set_queue_attribute(connection, queue_url, 'DelaySeconds', delivery_delay,
                                  check_mode=check_mode) or changed
    changed = set_queue_attribute(connection, queue_url, 'ReceiveMessageWaitTimeSeconds', receive_message_wait_time,
                                  check_mode=check_mode) or changed
    changed = set_queue_attribute(connection, queue_url, 'Policy', policy,
                                  check_mode=check_mode) or changed
    changed = set_queue_attribute(connection, queue_url, 'RedrivePolicy', redrive_policy,
                                  check_mode=check_mode) or changed
    changed = set_queue_attribute(connection, queue_url, 'ContentBasedDeduplication', fifo_content_based_deduplication,
                                  check_mode=check_mode) or changed
    return changed


def set_queue_attribute(connection, queue_url, attribute, value, check_mode=False):
    if not value and value != 0:
        return False

    # Get the current value, or return an empty string.
    try:
        existing_value = connection.get_queue_attributes(QueueUrl=queue_url, AttributeNames=[attribute])['Attributes'][attribute]
    except:
        existing_value = ''

    # convert dict attributes to JSON strings (sort keys for comparing)
    if attribute in ['Policy', 'RedrivePolicy']:
        value = json.dumps(value, sort_keys=True)
        if existing_value:
            existing_value = json.dumps(json.loads(existing_value), sort_keys=True)

    if str(value).lower() != existing_value:
        if not check_mode:
            result = connection.set_queue_attributes(QueueUrl=queue_url, Attributes={attribute:str(value)})
        return True

    return False


def delete_sqs_queue(connection, module):
    queue_name = module.params.get('name')

    result = dict(
        region=module.params.get('region'),
        name=queue_name,
    )

    try:
        queue_url = connection.get_queue_url(QueueName=queue_name)['QueueUrl']
        if queue_url:
            if not module.check_mode:
                connection.delete_queue(QueueUrl=queue_url)
            result['changed'] = True

        else:
            result['changed'] = False

    except botocore.exceptions.ClientError:
        result['msg'] = 'Failed to delete sqs queue due to error: ' + traceback.format_exc()
        module.fail_json(**result)
    else:
        module.exit_json(**result)


def main():
    argument_spec = ec2_argument_spec()
    argument_spec.update(dict(
        state=dict(default='present', choices=['present', 'absent']),
        name=dict(required=True, type='str'),
        queue_type=dict(default='standard', choices=['standard', 'fifo']),
        fifo_content_based_deduplication=dict(default=False, type='bool'),
        default_visibility_timeout=dict(type='int'),
        message_retention_period=dict(type='int'),
        maximum_message_size=dict(type='int'),
        delivery_delay=dict(type='int'),
        receive_message_wait_time=dict(type='int'),
        policy=dict(type='dict', required=False),
        redrive_policy=dict(type='dict', required=False),
    ))

    module = AnsibleAWSModule(
        argument_spec=argument_spec,
        supports_check_mode=True)

    if not HAS_BOTO3:
        module.fail_json(msg='boto3 and botocore are required for this module')

    try:
        region, ec2_url, aws_connect_params = get_aws_connection_info(module, boto3=True)
        connection = boto3_conn(module, conn_type='client', resource='sqs', region=region, endpoint=ec2_url, **aws_connect_params)
    except botocore.exceptions.NoRegionError:
        module.fail_json(msg=("Region must be specified as a parameter in AWS_DEFAULT_REGION environment variable or in boto configuration file."))

    state = module.params.get('state')
    if state == 'present':
        create_or_update_sqs_queue(connection, module)
    elif state == 'absent':
        delete_sqs_queue(connection, module)


if __name__ == '__main__':
    main()
