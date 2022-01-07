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

def generate_call_queue():
    
    agentQueryResponse = dynamodb_r.Table("AgentQueueFIFO").query(
        KeyConditionExpression=Key('pk').eq("Agents"),
        FilterExpression=Attr('AgentStatus').eq("available"),
        ConsistentRead=True
    )
    #print(response['Items'])

    for agent in agentQueryResponse['Items']:
        print("----------")
        print(agent['Languages'])
        now = datetime.now()
        #dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        dt_string = now.strftime("%Y/%m/%d-%H:%M:%S.%f")
        agentQueueTransactionItems=[]
        updateAgentStatusTransactionItem={}
        for language in agent['Languages']:
            print(language)
            
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
            #print("-----2-----")
            #print(updateTransactionItem)
            agentQueueTransactionItems.append(updateTransactionItem)

            if not updateAgentStatusTransactionItem:
                updateAgentStatusTransactionItem['Update']=dict({'TableName': 'AgentQueueFIFO'})
                key={}
                key['pk']=dict({'S':"Agents"})
                key['sk']=dict({'S': "Agent#"+agent['AgentName']})
                updateAgentStatusTransactionItem['Update']['Key']=key
                #updateAgentStatusTransactionItem['Update']['ConditionExpression'] = "AgentStatus = :unassigned"
                updateAgentStatusTransactionItem['Update']['UpdateExpression'] = "SET AgentStatus = :queued"
                updateAgentStatusTransactionItem['Update']['ExpressionAttributeValues']= dict(
                    {
                        #':unassigned':dict({'S' : 'un-assigned'}),
                        ':queued':dict({'S' : 'queued'})
                    }
                )
                agentQueueTransactionItems.append(updateAgentStatusTransactionItem)

        #print(transactionItem)
        print(agentQueueTransactionItems)
        #print("-----3-----")
        try:
            transactionResponse = dynamodb_c.transact_write_items(TransactItems=agentQueueTransactionItems)
        except dynamodb_c.exceptions.TransactionCanceledException as e:
            print(e.response)
        print("wrote to db in transaction")
        #print(transactionResponse)
        sleep(random.randint(1, 15))


if __name__ == '__main__':
    #create_agentQueue_table()
    #print("Table status:", agent_queue_table.TableStatus)
    #agentList=generate_agents(8)
    generate_call_queue()
    # #dynamodb.Table("AgentQueueFIFO").put_item(Item=call_queue_item)

