from typing import ItemsView
import boto3
import random
from faker import Factory
from time import sleep
from datetime import datetime
import uuid

fake = Factory.create()

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
    print(response)


def generate_queue_version_depth():
    language_list = ['English','French','Spanish']
    gender_list = ['F','M']
    for gender in gender_list:
        for language in language_list:
            response = dynamodb.put_item(TableName='AgentQueueFIFO', 
                Item={
                    'pk' : {'S':'AllQueues'},
                    'sk' : {'S':"Q#"+language+"#"+gender},
                    'QueueVersionId' : {'N': '0'},
                    'QueueDepth' : {'N' : '0'}
                }
            )



def generate_agents(numOfAgents):
    #AgentQueueFIFOTable= dynamodb.Table('AgentQueueFIFO')
    language_list = ['English','French','Spanish']
    agentList = []
    for i in range(numOfAgents):
        agent={}
        agent_profile = fake.profile()
        agent['agent_name'] = agent_profile['name'].split(' ',2)[0]
        agent['gender'] = agent_profile['sex']
        #agent['language'] = random.choice(language_list)
        num_of_languages=random.randint(1, 3)
        #agent['language'] = random.choices(language_list,k=num_of_languages)
        agent['language'] = random.sample(language_list,k=num_of_languages)
        #print(agent['language'])
        #todo - change Languages from map to list.
        print(agent)
        response = dynamodb.put_item(TableName='AgentQueueFIFO', 
            Item={
                'pk' : {'S':'Agents'},
                'sk' : {'S':"Agent#"+agent['agent_name']},
                'AgentName' : {'S': agent['agent_name']},
                'AgentID' : {'S' : str(uuid.uuid4())},
                'Gender' : {'S' : agent['gender']},
                'Languages' : {'SS' : agent['language']},
                'AgentStatus' : {'S': 'available'}
            }
        )
        print("Wrote agent to table - " + str(response['ResponseMetadata']['HTTPStatusCode']))         
        #print(response['ResponseMetadata']['HTTPStatusCode'])


if __name__ == '__main__':
    #create_agentQueue_table()
    #print("Table status:", agent_queue_table.TableStatus)
    agentList=generate_agents(5)
    generate_queue_version_depth()
    # #dynamodb.Table("AgentQueueFIFO").put_item(Item=call_queue_item)

