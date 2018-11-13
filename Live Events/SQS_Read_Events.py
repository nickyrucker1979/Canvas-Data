import boto3
import json
import pandas as pd
from pprint import pprint
import config_SQS as csqs
import exasol as e
import config_Exasol as ec
import time

queue_url = csqs.sqs_base_url + '-logins'
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
exasol_import_db = 'CANVAS_LIVE_EVENTS_STG.LOGINS'

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
                # 'Body': body,
                'Action': bodydata['action'],
                'UserIdIDM': bodydata['actor']['extensions']['com.instructure.canvas']['user_login'] ,
                'UserIdCanvas': bodydata['actor']['id'],

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

    idx = 10
    while idx > 0:

        time.sleep(1)
        messages = get_messages(num_of_records=idx)
        # while idx > 0 keep looping through until all messages are received
        if len(messages) == 0:
            idx=0
            break;
        else:
        # idx = len(messages)
        # pprint(messages)
            df = pd.DataFrame(messages).reindex()

            for index, row in df.iterrows():
                rhandle = row['ReceiptHandle']
                delete_message(rhandle)

            message_df = df[[
                'Action',
                'EventTime',
                'UserIdCanvas',
                'UserIdIDM'
            ]]
            print('')
            print(message_df)
            print('-----')
            message_df = message_df.reindex()
            exaconnect.writePandas(message_df, exasol_import_db)
