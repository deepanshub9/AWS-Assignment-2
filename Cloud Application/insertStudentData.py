import json
import boto3

# Create a DynamoDB object for using the AWS SDK
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
# fetch the DynamoDB object to select our table
table = dynamodb.Table('studentData') #table_name
#SNS define
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:425059748078:studentDataTopic'
# Define the handler function that the Lambda service will use as an entry point
def lambda_handler(event, context):
    # Extract values from the event object we got from the Lambda service and store in variables
    student_id = event['studentid']
    name = event['name']                                        #json format
    student_class = event['class']
    age = event['age']
    
    # Write student data to the DynamoDB table and save the response in a variable
    response = table.put_item(
        Item={
            'studentid': student_id,
            'name': name,
            'class': student_class,
            'age': age
        }
    )
    
      # Publish notification to SNS service
    sns_point = {
        'studentid': student_id,
        'name': name,
        'class': student_class,
        'age': age
    }
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=json.dumps(sns_point),
        Subject='New Student Added'
    )
    
    # JSON object Return a properly formatted 
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully saved Student and notification sent!')
    }













