import json
import logging
import requests
import time
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic

logging.basicConfig(level=logging.INFO)

# Kafka configurations
kafka_broker = 'localhost:9092'
group_id = 'agent_group'

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka Admin client setup to create topics
admin_client = KafkaAdminClient(
    bootstrap_servers=[kafka_broker],
    client_id='admin_client'
)

# Kafka Consumer setup
def create_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[kafka_broker],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id
    )

def send_message(topic, message):
    future = producer.send(topic, message)
    result = future.get(timeout=10)
    logging.info(f"Sent message to {topic}: {message} with result: {result}")

def create_topic(topic_name):
    try:
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logging.info(f"Created topic: {topic_name}")
    except Exception as e:
        logging.info(f"Topic {topic_name} already exists or error creating topic: {str(e)}")

def ensure_topic_exists(topic_name):
    while True:
        try:
            topics = admin_client.list_topics()
            if topic_name in topics:
                break
            else:
                logging.info(f"Waiting for topic {topic_name} to be available...")
                time.sleep(1)
        except Exception as e:
            logging.error(f"Error while checking for topic {topic_name}: {str(e)}")
            time.sleep(1)

def receive_message(topic, callback):
    consumer = create_consumer(topic)
    for message in consumer:
        callback(message.value)
        break

# POST request to APIs and get response from each API
def sophisticated_query_agent(subquery):
    response = requests.post("http://localhost:5002/sophisticated_query", json={"query": subquery})
    logging.info(f"Sophisticated Query Agent response: {response.status_code} - {response.text}")
    return response.json()

def simulation_agent(subquery):
    response = requests.post("http://127.0.0.1:5004/simulation", json={"query": subquery})
    logging.info(f"Simulation Agent response: {response.status_code} - {response.text}")
    return response.json()

def ds_doc_agent(subquery):
    response = requests.post("http://127.0.0.1:5001/ds_doc", json={"query": subquery})
    logging.info(f"DS Doc response: {response.status_code} - {response.text}")
    return response.json()

# Routing function gets the intent and directs to the specific API, creates a topic, and communicates between agents
def route_query(intentions):
    topic = f"{intentions[0]['intent']['name']}_agent_topic"
    create_topic(topic)
    ensure_topic_exists(topic)

    for i, item in enumerate(intentions):
        intent_name = item['intent']['name']
        subquery = item['subquery']

        if intent_name == "sophisticated_query":
            response = sophisticated_query_agent(subquery)
        elif intent_name == "simulation":
            response = simulation_agent(subquery)
        elif intent_name == "ds_doc":
            response = ds_doc_agent(subquery)
        else:
            logging.error(f"Unknown intent: {intent_name}")
            continue

        message = {
            "from_agent": intent_name,
            "response": f"This is my response: {response}"
        }
        send_message(topic, message)

        if i < len(intentions) - 1:
            def callback(message, next_agent=intentions[i + 1]['intent']['name']):
                logging.info(f"Received message from {intent_name}: {message}")
                subquery = f"Processed by {intent_name}: {message['response']}"
                if next_agent == "simulation":
                    simulation_callback(subquery, topic)
                elif next_agent == "sophisticated_query":
                    sophisticated_query_callback(subquery, topic)
                elif next_agent == "ds_doc":
                    ds_doc_callback(subquery, topic)
                else:
                    logging.error(f"Unknown next agent: {next_agent}")

            receive_message(topic, callback)

def simulation_callback(subquery, topic):
    response = simulation_agent(subquery)
    send_message(topic, {"final_response": response})

def sophisticated_query_callback(subquery, topic):
    response = sophisticated_query_agent(subquery)
    send_message(topic, {"final_response": response})

def ds_doc_callback(subquery, topic):
    response = ds_doc_agent(subquery)
    send_message(topic, {"final_response": response})

# Example usage
intentions = [
    {'order': '1', 'subquery': 'Get content design created today.', 'intent': {'name': 'sophisticated_query', 'similarity_score': 0.5147361915064453}},
    {'order': '2', 'subquery': 'Launch simulation about this content design.', 'intent': {'name': 'simulation', 'similarity_score': 0.7537923886072124}}
]

route_query(intentions)
