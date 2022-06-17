from typing import ItemsView
import boto3
import random
from faker import Factory
from time import sleep
from datetime import datetime
import uuid

fake = Factory.create()

dynamodb = boto3.client('dynamodb')

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
    language_list = ['English','French','Spanish']
    agentList = []
    for i in range(numOfAgents):
        agent={}
        agent_profile = fake.profile()
        agent['agent_name'] = agent_profile['name'].split(' ',2)[0]
        agent['gender'] = agent_profile['sex']
        num_of_languages=random.randint(1, 3)
        agent['language'] = random.sample(language_list,k=num_of_languages)
        #print(agent)
        response = dynamodb.put_item(TableName='AgentQueueFIFO', 
            Item={
                'pk' : {'S':'Agents'},
                #using agent_name in sort key to make it easy to understand. Uniqueness is not guarenteed. Use AgentID instead.
                'sk' : {'S':"Agent#"+agent['agent_name']},
                'AgentName' : {'S': agent['agent_name']},
                'AgentID' : {'S' : str(uuid.uuid4())},
                'Gender' : {'S' : agent['gender']},
                'Languages' : {'SS' : agent['language']},
                'AgentStatus' : {'S': 'available'}
            }
        )
        print("Added " + str(agent) + " to pool")


if __name__ == '__main__':
    #Using 5 for agent pool size
    generate_agents(5)
    generate_queue_version_depth()