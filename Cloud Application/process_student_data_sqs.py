

import json
import boto3            #import AWS SDK for python(boto3)
from botocore.exceptions import ClientError

# Initialize the DynamoDB and for SNS and SQS clients with region mention
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
sns_topic_data = boto3.client('sns', region_name='us-east-1')

# Configuration with SNS table and AWS arn link so it's understand where notification generated
STUDENT_TABLE = 'studentData'
NOTIFICATION_TOPIC = 'arn:aws:sns:us-east-1:425059748078:studentDataTopic'

# Main Lambda function handler
def lambda_handler(event, context):
    """
    AWS Lambda handler triggered by SQS messages.
    Processes each message, saves data to DynamoDB, and sends notifications.
    """
    for record in event.get('Records', []):
        try:
            # Parse SQS message body inside of sqs client
            student_info_data = json.loads(record['body'])
            print(f"New student data received: {student_info_data}")
            
             #  show Save to database when function work till here.
            save_to_dynamodb(student_info_data)
            
           # Notify when user added in the form and notify to sqs and email field
            notification_sns(student_info_data)
        
        except (json.JSONDecodeError, KeyError) as parse_error:
            print(f"Failed to parse message: {parse_error}")
       
        except Exception as general_error:
            print(f"Unexpected error: {general_error}")

    return {"statusCode": 200, "body": "Messages processed successfully"}


#save database response
def save_to_dynamodb(student_data):
    """
    Saves student data to DynamoDB.
    """
    try:
        table = dynamodb.Table(STUDENT_TABLE)
        table.put_item(Item=student_data)
        print("Student data saved successfully")
    except ClientError as db_error:
        print(f"Error saving to DynamoDB: {db_error.response['Error']['Message']}")
        raise


#sns notification function
def notification_sns(student_data):
    """
    Notification sent to the SNS topic with student data.
    """
    try:
        sns_topic_data.publish(
            TopicArn=NOTIFICATION_TOPIC,
            Subject="Student Record Update",
            Message=json.dumps(student_data, indent=2)
        )
        print("Notification sent via SNS.")
    except ClientError as sns_error:
        print(f"Error sending notification: {sns_error.response['Error']['Message']}")
        raise



