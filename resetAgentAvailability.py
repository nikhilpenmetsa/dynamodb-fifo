import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

dynamodb_r = boto3.resource('dynamodb')

def reset_all_attending_to_avail():
    agentQueryResponse = dynamodb_r.Table("AgentQueueFIFO").query(
        KeyConditionExpression=Key('pk').eq("Agents"),
        FilterExpression=Attr('AgentStatus').eq("attendingCall")
    )
    
    for agent in agentQueryResponse['Items']:
        print("Updating status for " + agent['AgentName'])
        updateItemResponse = dynamodb_r.Table("AgentQueueFIFO").update_item(
            Key={
                'pk': 'Agents',
                'sk': agent['sk']
            },
            UpdateExpression="SET AgentStatus = :available",
            ExpressionAttributeValues={
                ':available' : 'available'
            }
        )
        print(updateItemResponse)

if __name__ == '__main__':
    reset_all_attending_to_avail()

