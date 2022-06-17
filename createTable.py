import boto3


dynamodb = boto3.client('dynamodb')

def create_agentQueue_table():

    response = dynamodb.create_table(
        TableName='AgentQueueFIFO',
        KeySchema=[
            {
                'AttributeName': 'pk',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'sk',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'pk',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'sk',
                'AttributeType': 'S'
            },

        ],
        BillingMode='PAY_PER_REQUEST'
       
    )

    print("Created table " + response['TableDescription']['TableName'] + " successfully")


if __name__ == '__main__':
    create_agentQueue_table()
