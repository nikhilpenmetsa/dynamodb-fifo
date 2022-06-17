import boto3


dynamodb = boto3.client('dynamodb')

def delete_agentQueue_table():

    response = dynamodb.delete_table(
        TableName='AgentQueueFIFO'
    )
    print("Deleted table " + response['TableDescription']['TableName'] + " successfully")

if __name__ == '__main__':
    delete_agentQueue_table()
