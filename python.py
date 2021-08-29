import json
import boto3
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        dead_queue_url = "[Dead Error Queue Endpoint URL]"
        queue_url = "[Queue Endpoint URL]"
        
        sqs = boto3.client('sqs')
        
        while True:
            receive_response = sqs.receive_message(
                QueueUrl=dead_queue_url,
                AttributeNames=[
                    'All'
                ],
                MessageAttributeNames=[
                    'string',
                ],
                MaxNumberOfMessages=10,
                VisibilityTimeout=10,
                WaitTimeSeconds=10
            )
            
            messages = receive_response["Messages"]
            
            if not messages:
                break
            
            for message in messages:
                body = message["Body"]
        
                send_response = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=body
                )
                logger.info("send message. body: " + body)
                
                status_code = send_response["ResponseMetadata"]["HTTPStatusCode"]
                
                if status_code == 200:
                    delete_response = sqs.delete_message(
                        QueueUrl=dead_queue_url,
                        ReceiptHandle=message["ReceiptHandle"]
                    )
                    
                    delete_status_code = delete_response["ResponseMetadata"]["HTTPStatusCode"]
                    if delete_status_code != 200:
                        logger.warn("failed delete message. status code: " + str(delete_status_code))
                else:
                    logger.warn("failed send message. status code: " + str(status_code))
        
        return {
            'statusCode': 200,
            'body': json.dumps('success!')
        }

    except Exception as e:
        logger.error(e)
