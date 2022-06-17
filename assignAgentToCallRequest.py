
import boto3
import random
from faker import Factory
from time import sleep
from datetime import datetime
import json
import sys

from boto3.dynamodb.conditions import Key

dynamodb_r = boto3.resource('dynamodb')
dynamodb_c = boto3.client('dynamodb')

# peek all queues
def getAllQueueMetadata():
    language_list = ['English','French','Spanish']
    gender_list = ['F','M']
    allQueuesTransactionItems=[]

    for gender in gender_list:
        for language in language_list:
            getTransactionItem={}
            getTransactionItem['Get']=dict({'TableName': 'AgentQueueFIFO'})
            
            key={}
            key['pk']=dict({'S':'AllQueues'})
            key['sk']=dict({'S':"Q#"+language+"#"+gender})
            getTransactionItem['Get']['Key']=key
            allQueuesTransactionItems.append(getTransactionItem)

    try:
        transactionResponse = dynamodb_c.transact_get_items(TransactItems=allQueuesTransactionItems)
    except dynamodb_c.exceptions.TransactionCanceledException as e:
        print(e.response)

    queueDepthVersionMap={}
    for queueDetails in transactionResponse['Responses']:
        item=queueDetails['Item']
        queueDepthVersionMap[item['sk']['S']]=dict({'QueueVersionId':item['QueueVersionId']['N'],'QueueDepth':item['QueueDepth']['N']})
    print("Current queue versions and depths:")
    print(json.dumps(queueDepthVersionMap,sort_keys=True, indent=4))
    return queueDepthVersionMap

def getFirstAgentQueueDetails(criteria):
    #query and return first item.get launguage(s)
    response = dynamodb_r.Table("AgentQueueFIFO").query(
        KeyConditionExpression=Key('pk').eq(criteria),
        ConsistentRead=True
    )
    if response['Items']:
        print("Found " , len(response['Items']), " available agent(s) for " +criteria)
        return response['Items'][0]
    else:
        print("No agent available for " + criteria)        

def assignFirstAgent(agentQueueItem,currentQueuesState):
    print("First matched agent queue message: " + agentQueueItem['AgentName'] + "  available since " + agentQueueItem['sk'])
    language_list = list(agentQueueItem['Languages'])
    print("Agent " + agentQueueItem['AgentName'] + " is in " + str(len(language_list)) + " queue(s)\n")
    gender = agentQueueItem['Gender']
    assignAgentTransactionItems=[]
    updateAgentStatusTransactionItem ={}
    for language in language_list:
       
        #delete item for each language
        deleteTransactionItem={}
        deleteTransactionItem['Delete']=dict({'TableName': 'AgentQueueFIFO'})
        deleteKey={}
        deleteKey['pk']=dict({'S':"Q#"+language+"#"+gender})
        deleteKey['sk']=dict({'S':agentQueueItem['sk']})
        deleteTransactionItem['Delete']['Key']=deleteKey

        print("Preparing transaction - Deleting agent queue message for " + agentQueueItem['AgentName'] + " and " + language )
        assignAgentTransactionItems.append(deleteTransactionItem)

        updateTransactionItem={}
        updateTransactionItem['Update']=dict({'TableName': 'AgentQueueFIFO'})
        updateKey={}
        updateKey['pk']=dict({'S':"AllQueues"})
        updateKey['sk']=dict({'S':"Q#"+language+"#"+gender})
        updateTransactionItem['Update']['Key']=updateKey
        updateTransactionItem['Update']['ConditionExpression'] = "QueueVersionId = :lastKnownQueueVersionId"
        updateTransactionItem['Update']['UpdateExpression'] = "SET QueueVersionId = QueueVersionId + :incr ADD QueueDepth :decr"
        updateTransactionItem['Update']['ExpressionAttributeValues']= dict(
            {
                ':incr':dict({'N':'1'}),
                ':decr':dict({'N':'-1'}),
                ':lastKnownQueueVersionId':dict({'N' : currentQueuesState["Q#"+language+"#"+gender]['QueueVersionId']})
            }
        )
        print("Preparing transaction - Incrementing queue versionID and decrementing queue depth of Q#"+ language + "#" +agentQueueItem['Gender'])
        assignAgentTransactionItems.append(updateTransactionItem)
        
        if not updateAgentStatusTransactionItem:
            updateAgentStatusTransactionItem['Update']=dict({'TableName': 'AgentQueueFIFO'})
            key={}
            key['pk']=dict({'S':"Agents"})
            key['sk']=dict({'S': "Agent#"+agentQueueItem['AgentName']})
            updateAgentStatusTransactionItem['Update']['Key']=key
            #updateAgentStatusTransactionItem['Update']['ConditionExpression'] = "AgentStatus = :unassigned"
            updateAgentStatusTransactionItem['Update']['UpdateExpression'] = "SET AgentStatus = :attendingCall"
            updateAgentStatusTransactionItem['Update']['ExpressionAttributeValues']= dict(
                {
                    ':attendingCall':dict({'S' : 'attendingCall'})
                    #':assigned':dict({'S' : 'assigned'})
                }
            )
            print("Preparing transaction - Updating agent status to attendingCall")
            assignAgentTransactionItems.append(updateAgentStatusTransactionItem)

        #print(transactionItem)
    #print(assignAgentTransactionItems)

    try:
        transactionResponse = dynamodb_c.transact_write_items(TransactItems=assignAgentTransactionItems)
        print("Transaction successful. Agent " + agentQueueItem['AgentName'] + " is matched to the caller.\n")

    except dynamodb_c.exceptions.TransactionCanceledException as e:
        print(e.response)
        print(transactionResponse)
    #print("***Dequeued agent call queue items, updated queue depths,versions, updated agent availability in a transaction")
    #print(transactionResponse)

if __name__ == '__main__':
    
    # print('Argument List:', str(sys.argv))
    criteria = sys.argv[1]
    # Get queue versionId for optimistic locking
    currentQueuesState = getAllQueueMetadata()

    #query for specific criteria
    print("Query first matching agent for: " + criteria)
    firstAvailAgent = getFirstAgentQueueDetails(criteria)

    #dequeue - remove item and related items, decrement queue depths, condition check
    if firstAvailAgent:
        assignFirstAgent(firstAvailAgent,currentQueuesState)

