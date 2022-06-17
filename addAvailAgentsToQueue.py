from logging import error
from typing import ItemsView
import boto3
import random
from faker import Factory
from time import sleep
from datetime import datetime
import uuid
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

fake = Factory.create()

dynamodb_c = boto3.client('dynamodb')
dynamodb_r = boto3.resource('dynamodb')

def generate_agent_queue():
    
    agentQueryResponse = dynamodb_r.Table("AgentQueueFIFO").query(
        KeyConditionExpression=Key('pk').eq("Agents"),
        FilterExpression=Attr('AgentStatus').eq("available"),
        ConsistentRead=True
    )

    for agent in agentQueryResponse['Items']:
        now = datetime.now()
        #dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        dt_string = now.strftime("%Y/%m/%d-%H:%M:%S.%f")
        agentQueueTransactionItems=[]
        updateAgentStatusTransactionItem={}
        print("Preparing transaction for agent " + agent['AgentName'])
        for language in agent['Languages']:
            #put item for each language
            transactionItem={}
            transactionItem['Put']=dict({'TableName': 'AgentQueueFIFO'})
            agentCallQueueItem={}
            agentCallQueueItem['pk']=dict({'S': "Q#"+language+"#"+agent['Gender']})
            agentCallQueueItem['sk']=dict({'S': dt_string})
            agentCallQueueItem['AgentName']=dict({'S': agent['AgentName']})
            agentCallQueueItem['AgentID']=dict({'S': agent['AgentID']})
            agentCallQueueItem['Gender']=dict({'S': agent['Gender']})
            agentCallQueueItem['Languages']=dict({'SS': list(agent['Languages'])})
            transactionItem['Put']['Item']=agentCallQueueItem
            #print(transactionItem)
            print("Preparing transaction - Adding " + agent['AgentName'] + " to " + language + " queue")
            agentQueueTransactionItems.append(transactionItem)

            #update queue depth for each language
            updateTransactionItem={}
            updateTransactionItem['Update']=dict({'TableName': 'AgentQueueFIFO'})

            queueDepthItem={}
            queueDepthItem['pk']=dict({'S':"AllQueues"})
            queueDepthItem['sk']=dict({'S': "Q#"+language+"#"+agent['Gender']})
            updateTransactionItem['Update']['Key']=queueDepthItem

            updateTransactionItem['Update']['UpdateExpression']= "SET QueueVersionId = if_not_exists(QueueVersionId, :zero) + :incr ADD QueueDepth :incr"
            updateTransactionItem['Update']['ExpressionAttributeValues']= dict(
                {
                    ':incr':dict({'N':'1'}),
                    ':zero':dict({'N':'0'})
                }
            )
            print("Preparing transaction - Incrementing queue versionID and queue depth of Q#"+ language + "#" +agent['Gender'])
            agentQueueTransactionItems.append(updateTransactionItem)

            if not updateAgentStatusTransactionItem:
                updateAgentStatusTransactionItem['Update']=dict({'TableName': 'AgentQueueFIFO'})
                key={}
                key['pk']=dict({'S':"Agents"})
                key['sk']=dict({'S': "Agent#"+agent['AgentName']})
                updateAgentStatusTransactionItem['Update']['Key']=key
                updateAgentStatusTransactionItem['Update']['UpdateExpression'] = "SET AgentStatus = :queued"
                updateAgentStatusTransactionItem['Update']['ExpressionAttributeValues']= dict(
                    {
                        #':unassigned':dict({'S' : 'un-assigned'}),
                        ':queued':dict({'S' : 'queued'})
                    }
                )
                print("Preparing transaction - Updating " + agent['AgentName'] + " status to queued")
                agentQueueTransactionItems.append(updateAgentStatusTransactionItem)

        try:
            transactionResponse = dynamodb_c.transact_write_items(TransactItems=agentQueueTransactionItems)
            print("Transaction successful\n")
        except dynamodb_c.exceptions.TransactionCanceledException as e:
            print(e.response)
            print(transactionResponse)
        sleep(random.randint(1, 2))


if __name__ == '__main__':
    generate_agent_queue()

