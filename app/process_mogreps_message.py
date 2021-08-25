import os
import json
import boto3
import urllib.request

#  Global Params
output_bucket = 'mogreps-uk-realtime'
sqs = boto3.client('sqs', region_name='eu-west-2')
s3_conn = boto3.client('s3')

def lambda_handler(event, context):
    
    status_code = 200
    print_message = None
    #  Read messages from SQS queue
    messages = sqs.receive_message(QueueUrl='https://sqs.eu-west-2.amazonaws.com/776262733296/mogreps-uk-input-queue', MaxNumberOfMessages=1)
    print ("messages::{}".format(messages))
    if 'Messages' not in messages:
        print_message = "No New messages"
    else:    
        [message] = messages['Messages']
        # print ("message::{}".format(message))
        notification = json.loads(message['Body'])
        print ("notification::{}".format(notification))
        message = json.loads(notification['Message'])
        key = message["key"]
        bucket =  message["bucket"]
        
        
        url = "https://s3.eu-west-2.amazonaws.com/" + bucket + "/" + key
        print ("url::{}".format(url))
        local_file_path = "/tmp/" + key
        local_file = urllib.request.urlretrieve(url, local_file_path) # save in this directory with same name
        s3_conn.upload_file(Filename=local_file_path, Bucket=output_bucket, Key=key)
        # file_bytes = s3_conn.get_object(Bucket=bucket,Key=key)["Body"].read()
        # s3_conn.put_object(Bucket=bucket, Key=key, Body=file_bytes)
        print_message = "Sucessfully processed key {}".format(key)


    return {
        'statusCode': status_code,
        'body': json.dumps(print_message)
    }
