
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
def getAllQueueDepths():
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

    #print(allQueuesTransactionItems)
    try:
        transactionResponse = dynamodb_c.transact_get_items(TransactItems=allQueuesTransactionItems)
    except dynamodb_c.exceptions.TransactionCanceledException as e:
        print(e.response)
    #print("got queue depths in a transaction")
    #print(transactionResponse['Responses'])

    queueDepthVersionMap={}
    for queueDetails in transactionResponse['Responses']:
        item=queueDetails['Item']
        queueDepthVersionMap[item['sk']['S']]=dict({'QueueVersionId':item['QueueVersionId']['N'],'QueueDepth':item['QueueDepth']['N']})
    print("***Captured all current queue versions and depths in a transaction")
    print(queueDepthVersionMap)
    return queueDepthVersionMap

def getFirstAgentQueueDetails(criteria):
    #query and return first item.get launguage(s), availdate
    response = dynamodb_r.Table("AgentQueueFIFO").query(
        KeyConditionExpression=Key('pk').eq(criteria)
    )
    #print("-----------")
    #print(response)
    if response['Items']:
        print("***Found " , len(response['Items']), " available agent for " +criteria + "... Assigning earliest available agent")
        print(response['Items'][0])
        return response['Items'][0]
    else:
        print("***No agent available for " + criteria)

def assignFirstAgent(agentQueueItem,currentQueuesState):
    language_list = list(agentQueueItem['Languages'])
    gender = agentQueueItem['Gender']
    #print(language_list)
    #print(gender)
    assignAgentTransactionItems=[]
    updateAgentStatusTransactionItem ={}
    for language in language_list:
        #print(language)
        
        #delete item for each language
        deleteTransactionItem={}
        deleteTransactionItem['Delete']=dict({'TableName': 'AgentQueueFIFO'})
        deleteKey={}
        deleteKey['pk']=dict({'S':"Q#"+language+"#"+gender})
        deleteKey['sk']=dict({'S':agentQueueItem['sk']})
        deleteTransactionItem['Delete']['Key']=deleteKey
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
            assignAgentTransactionItems.append(updateAgentStatusTransactionItem)

        #print(transactionItem)
    #print(assignAgentTransactionItems)

    #print("-----3-----")
    try:
        transactionResponse = dynamodb_c.transact_write_items(TransactItems=assignAgentTransactionItems)
    except dynamodb_c.exceptions.TransactionCanceledException as e:
        print(e.response)
    print("***Dequeued agent call queue items, updated queue depths,versions, updated agent availability in a transaction")
    #print(transactionResponse)

if __name__ == '__main__':
    
    # print('Argument List:', str(sys.argv))
    
    criteria = sys.argv[1]
    print(criteria)
    # criteria="Q#Spanish#F"
    #get all queue depths and versions in a trasaction.
    currentQueuesState = getAllQueueDepths()

    #query for specific criteria
    firstAvailAgent = getFirstAgentQueueDetails(criteria)
    if firstAvailAgent:
        assignFirstAgent(firstAvailAgent,currentQueuesState)
    #dequeue - remove item and related items, decrement queue depths, condition check

