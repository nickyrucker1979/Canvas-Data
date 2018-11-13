import boto3
import json
import pandas as pd
from pprint import pprint
import config_SQS as csqs
import exasol as e
import config_Exasol as ec
import simplejson

num_of_records = 1

def get_messages(is_json=True):
    sqsclient = boto3.client(
        'sqs',
        aws_access_key_id = csqs.access_key,
        aws_secret_access_key = csqs.secret_key,
        region_name = csqs.region_name
    )
    # Receive message from SQS queue
    response = sqsclient.receive_message(
        QueueUrl=csqs.sqs_url,
        AttributeNames=[
            'All'
        ],
        MaxNumberOfMessages=num_of_records,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    result = []
    # messages = response

    jdumps = json.dumps(response)
    messages = json.loads(jdumps)['Messages']
    # return messages
    # print(response)
    for msg in messages:
        # Parse the strings as JSON so that we can deal with them easier
        # if is_json:
        # msg = json.loads(msg)
        # else:
        #     body = msg.body
        # msgs = json.loads(msg)['Messages']
        msg_dict = {
            'ReceiptHandle': msg['ReceiptHandle'],
            'MessageId': msg['MessageId'],
            'Body': msg['Body'],
            'MessageAttributes': msg['MessageAttributes'],
            'EventName': msg['MessageAttributes']['event_name']['StringValue'],
            'Attributes': msg['Attributes'],
        }
        result.append(msg_dict)

    return result

if __name__ == '__main__':
    messages = get_messages()
    pprint(messages)
