import boto3
import json
import pandas as pd
from pprint import pprint
import config_SQS as csqs
import exasol as e
import config_Exasol as ec
import time

queue_url = csqs.sqs_base_url + '-submissions'
sqsclient = boto3.client(
    'sqs',
    aws_access_key_id = csqs.access_key,
    aws_secret_access_key = csqs.secret_key,
    region_name = csqs.region_name
)

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )
exasol_import_db = 'CANVAS_LIVE_EVENTS_STG.SUBMISSIONS'

def get_queue_size(name):
    sqsresource =  boto3.resource(
        'sqs',
        aws_access_key_id = csqs.access_key,
        aws_secret_access_key = csqs.secret_key,
        region_name = csqs.region_name
    )
    queue = sqsresource.get_queue_by_name(QueueName=name)
    queue_size = (queue.attributes['ApproximateNumberOfMessages'])
    return queue_size

def delete_message(msg):
    # print(msg)
    # # Delete received message from queue
    sqsclient.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=msg
    )

def get_messages(num_of_records, is_json=True):
    # Receive message from SQS queue
    response = sqsclient.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'All'
        ],
        MaxNumberOfMessages=num_of_records,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=1,
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
        if is_json:
           body = json.loads(msg['Body'])
           for d in body['data']:
               bodydata = d
        try:
            msg_dict = {
                'ReceiptHandle': msg['ReceiptHandle'],
                'MessageId': msg['MessageId'],
                # 'Body': bodydata,
                'Action': bodydata['action'],
                'UserIdCanvas': bodydata['actor']['id'],
                'SubmissionId': bodydata['object']['assignable']['id'],
                'Course': bodydata['group']['id'],
                # 'MessageAttributes': msg['MessageAttributes'],
                'EventName': msg['MessageAttributes']['event_name']['StringValue'],
                'EventTime': msg['MessageAttributes']['event_time']['StringValue'],
                # 'Attributes': msg['Attributes'],
            }

            result.append(msg_dict)

        except:
            # write to error file?
            continue;
    return result

if __name__ == '__main__':

    # queries the number of messages in the queue when initially polled
    num_of_messages_in_queue = get_queue_size('canvas-live-events-submissions')
    idx = int(num_of_messages_in_queue)
    print(idx)

    while idx > 0:

        time.sleep(1)
        # max number in a batch query is 10
        if idx > 10:
            x = 10
        else:
            x = idx
        messages = get_messages(num_of_records=x)

        # if we recieve an empty call, try again (some calls return empty results due to how AWS stores info in sqs)
        if len(messages) == 0:
            continue;
        else:
        # idx = len(messages)
            pprint(messages)
            df = pd.DataFrame(messages).reindex()
            for index, row in df.iterrows():
                rhandle = row['ReceiptHandle']
                delete_message(rhandle)

            message_df = df[[
                'Action',
                'Course',
                'EventName',
                'EventTime',
                'SubmissionId',
                'UserIdCanvas'

            ]]

            print('')
            print(df)
            # print(message_df)
            print('-----')
            message_df = message_df.reindex()
            print('')
            print(message_df)
            exaconnect.writePandas(message_df, exasol_import_db)

            # reduce the num of records left in queue by the num of messages returned in this loop
            idx -= len(messages)
            print(idx)
