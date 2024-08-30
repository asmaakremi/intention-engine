import numpy as np
import requests
import logging
import json
import os
from flask import Flask, request, jsonify
from semantic_router import Route, RouteLayer
from semantic_router.encoders import CohereEncoder
from kafka_utils import simulation_callback,sophisticated_query_callback,ds_doc_callback,sophisticated_query_agent,simulation_agent,ds_doc_agent, create_topic, ensure_topic_exists, send_message, receive_message

app = Flask(__name__)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
os.environ["COHERE_API_KEY"] = "rDOO5bMWauL868W6mYdCWlBydL4i4VvJTNYwbof6"

default_prompt = Route(
    name="default_prompt",
    utterances=[]
)
ds_doc = Route(
    name="3ds_doc",
    description="documentation, informative assitant how to use the 3dSearch and 6wtags withing to 3dExperience platform for simple users, developer and admins",
    Role=["internalDS","systemAdmin","3dswymer"],
    utterances=[
        "What are the steps and options available to administrators for customizing the search and tagging functionalities within the 3DEXPERIENCE platform?",
        "How do I manage and refine searches within the 3DEXPERIENCE platform?",
        "How do I manage and refine searches within the 3DEXPERIENCE platform?",
        "How to select and customize view modes in 3DSearcht",
        "How to handle search settings and errors in 3DSearch:",
        "How to utilize search result actions like tagging, downloading, and editing in 3DSearch",
        "How to sort and export search results in 3DSearch",
        "How to manage column customizations in Datagrid view in 3DSearch",
        "How to select multiple objects in 3DSearch",
        "What is the purpose of the topbar in the 3DEXPERIENCE platform",
        "How to refine search queries using advanced search options in the topbar",
        "How to select and customize view modes in 3DSearch",
        "can i use Advanced Search",
        "How do I refine searches within the 3DEXPERIENCE platform, including performing and refining searches, selecting and customizing view modes, and utilizing search result actions?",
        "What are the key features of the 6WTags functionality, including filtering search results, managing tags in widgets, and utilizing app-specific integrations?",
        "What are the steps and options available to administrators for customizing the search and tagging functionalities within the 3DEXPERIENCE platform?",
        "How do I manage and filter content using six categories: What, Who, When, Where, How, and Why?",
        "Retrieve the latest updates in the documentation about ",
    ],
)

sophisticated_query = Route(
    name="sophisticated_query",
    description="Responsible for lauching the search and filter using natural language. Users can search for objects and filter by Creator, by Responsible, Creation date, Modification date, Type of object, Content, Description, Title...",
    Role=["systemAdmin","3dswymer"],
    utterances=[
        "Show all documents created by John Doe in January 2024.",
        "List all 3D shapes modified before July 10, 2024, by any engineer.",
        "Give me all social content titled 'Team Building Exercise' created by Jane",
        "Retrieve all documents with a .docx extension edited by Jane.",
        "Find posts from 2024 that have received more than 10 likes.",
        "Display all personnel in the Data Scientist role who have created documents this year.",
        "Show me every vpmreference product updated in the last month.",
        "Which documents related to 'AI Project' were last modified by a Vice President?",
        "List all the ideas that have no comments as of today.",
        "Find all profiles of engineers who modified any social content since June 2024.",
        "What are the latest documents created by Idir",
        "Show all products that have been labeled as prototypes and modified this week.",
        "Retrieve details on social posts made by Alice that include 'market trends'.",
        "retrieve 3D model created on February 1, 2024",
        "List all custom documents created last year.",
        "Find all users who posted ideas related to 'sustainability' in 2024.",
        "Show me documents with more than 5 revisions made in the first quarter of 2024.",
        "Which VpmReferences are classified under the root content structure?",
        "Retrieve all social content created by the R&D department.",
        "Display the document titled 'Safety Protocols'.",
        "List all physical products created by any member of the Strategy & Management team.",
        "Who modified the most documents in the month of May 2024?",
        "Find all posts with exactly 2 endorsements that discuss 'new technology'.",
        "Show all business roles related to the 'Engineer' category updated last week.",
        "Retrieve all documents linked to 'Product Design' that were last modified yesterday.",
        "List everyone who created a PLMDocCustom document in March 2024.",
        "Which documents about 'Project Alpha' were last reviewed by software engineers?",
        "Find all social posts from 2024 discussing 'team collaboration'.",
        "Display all physical products classified under Intermediate content structure created in 2024.",
        "List all VPMReferences created by people in the 'Data Scientist' role.",
        "Retrieve the latest modified VPMReference by any Vice President.",
        "Show me all personnel changes within the Engineering department this year.",
        "Which social content received the highest number of likes last month?",
        "Find documents with 'confidential' in their descriptions updated by strategy managers.",
        "Display all posts made by any person named 'Tom' that have at least 3 comments.",
        "List all engineering documents that include the term 'prototype' in their title.",
        "show documents related to 'Regulatory Compliance' in 2024?",
        "Find all document with extension 'doc' or 'pdf'.",
        "Display the documents created by the Software Engineer in 2024.",
        "Filter all content by Creator, Creation Date, Responsible and Type",
        "Search all posts, products, ideas by Creator, Creation Date, Responsible and Type",
        "Provide guidance on using the search",
        "retrieve  models named begins with 3d that are compatible with version X.Y "
    ],
)

simulation = Route(
    name="simulation",
    description="An Act assistant, that do simulations of object, receive in input and object for example a products, a description, and convert it to 3d design or do a simulation of it",
    Role=["systemAdmin","3dswymer"],
    utterances=[
        "Simulate airflow around the new car design model.",
        "Run a stress test on the bridge structure model from the latest dataset.",
        "Initiate a thermal simulation for the VPMReference of the jet engine.",
        "Show me the simulation results of the earthquake resistance test for building model B23.",
        "Apply a 10 percent increase in load to the wing structure and rerun the simulation.",
        "Create a 3D simulation showing the wear and tear over 5 years for the gear assembly.",
        "Display the vibration analysis for the prototype machine model under operational conditions.",
        "Simulate water flow through the new pipe design and identify potential pressure drop points.",
        "Run a dynamic simulation for the robotic arm movements when lifting a 100 kg weight.",
        "Perform a crash test simulation for the latest car chassis design.",
        "Generate a color stress map on the VPMReference of the suspension bridge under maximum load.",
        "Show a real-time simulation of the assembly line for product X using its 3D model.",
        "Execute a fatigue analysis on the turbine blades model based on current operational data.",
        "Simulate the dispersion pattern of particles in the air filter system.",
        "Preview the heat dissipation simulation for the newly designed laptop cooling system.",
        "Conduct a light exposure simulation on the solar panel array model.",
        "Perform a simulation to test the new packaging design's resistance to shock and drops.",
        "Create a simulation of the airflow in the HVAC system for the commercial building model.",
        "Run a fluid dynamics simulation for the prototype water pump.",
        "Simulate the electrical conductivity of the new semiconductor design.",
        "Preview the deformation under load for the composite material sample.",
        "Initiate a magnetic field simulation for the electric motor model.",
        "Perform a wind tunnel simulation on the scale model of the high-speed train.",
        "Display the simulation of sound wave propagation through the new theater design.",
        "Simulate the effect of different soil types on the foundation model of the skyscraper.",
        "Run a multi-phase flow simulation for the oil pipeline model under varying pressures.",
        "Show the thermal expansion simulation for the metal alloy under extreme heat.",
        "Perform a particle collision simulation in the new accelerator model.",
        "Create a 3D simulation of light absorption by the new photovoltaic material.",
        "Simulate the mechanical behavior of the joint assembly under repetitive movements.",
        "Execute a corrosion simulation for the underwater hull design of the ship.",
        "Run an aerodynamics simulation for the new drone design at different wind speeds.",
        "Display a simulation of the wireless signal coverage for the new smartphone design.",
        "Simulate the operational efficiency of the hydraulic system under full load.",
        "Perform a shadow analysis on the urban development model during summer solstice.",
        "Initiate a resonance frequency simulation for the newly designed guitar.",
        "Show the simulation of fluid interaction with different impeller designs in the mixer.",
        "Run a granular flow simulation for the hopper design in the food processing plant.",
        "Simulate the dynamic response of the suspension system when driving over rough terrain.",
        "Perform a virtual crash test for the latest helmet design.",
        "Simulate the performance of the retrieved models",
    ],
)

routes = [ ds_doc,sophisticated_query, simulation]
# routes = [default_prompt, ds_doc, federated_search, simulation]

os.environ["COHERE_API_KEY"] = "rDOO5bMWauL868W6mYdCWlBydL4i4VvJTNYwbof6"
encoder = CohereEncoder(cohere_api_key=os.getenv("COHERE_API_KEY"))
rl = RouteLayer(encoder=encoder, routes=routes)


def get_dag(query):
    prompt = f"""
    You are an intelligent agent responsible for analyzing user queries. Your task is to identify underlying tasks or intents within the provided user query and decompose it into subqueries if necessary. Analyze the user query below to determine if it represents a single intent with multiple conditions or multiple distinct intents. If the query represents a single intent or single intent with multiple conditions, consolidate all conditions into one subtask description. If there are multiple distinct intents, break them down into sequential subtasks.

    Produce a structured response in the form of an array of dictionaries, where each dictionary contains keys 'order' and 'subquery'. Each dictionary should detail the sequential order of execution and the corresponding subquery derived from the user query. Ensure that the subqueries are listed in the logical sequence required to fulfill the overall intent of the user query.
    Return only the desired output and do not add any extra text.

    # Example user query decomposing:
    Natural Language: "Can you guide me through the process of using metadata for 3D searches and then perform a search for models tagged as 'eco-friendly' that need simulation testing?"
    Output:
    [
        {{"order": "1", "subquery": "Guide me through the process of using metadata for 3D searches."}},
        {{"order": "2", "subquery": "Perform a search for models tagged as 'eco-friendly'."}},
        {{"order": "3", "subquery": "Launch simulation of these models."}}
    ]

    Natural Language: "Retrieve products, posts, and ideas created yesterday by Anne."
    Output:
    [
        {{"order": "1", "subquery": "Retrieve products, posts, and ideas created yesterday by Anne."}}
    ]
    Based on the natural language query: {query} Produce a structured response in the form of an array of dictionaries,
    Return only the desired output and do not add any extra text or explanation.
    """

    processed_query = call_llm(prompt)
    # logging.info(f" LLM Decomposer query: {processed_query}")
    try:
        dag = json.loads(processed_query)
        logging.info(f"DAG: {processed_query}")

    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
        raise ValueError(f"Failed to decode JSON. Error: {e}")
    
    # logging.info(f"DAG: {dag}")
    return dag

def call_llm(prompt) :
    url = 'http://px101.prod.exalead.com:8110/v1/chat/completions'
    headers = {
        'Authorization': 'Bearer vtYvpB9U+iUQwl0K0MZIj+Uo5u6kilAZJdgHGVBEhNc=',
        'Content-Type': 'application/json'
    }
    messages = [{"role": "user", "content": prompt}]
    payload = {
        "model":"meta-llama/Meta-Llama-3-8B-Instruct",
        "messages": messages,  
        "max_tokens": 500,
        "top_p": 1,
        "stop": ["string"],
        "response_format": {
            "type": "text", 
            "temperature": 0.7
        }
    }
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        generated_response = response.json()['choices'][0]['message']['content'].strip()
        # logging.info(f"LLM response: {generated_response}")
        return generated_response
    else:
        return f"Failed to generate response. Status code: {response.status_code}\nResponse: {response.text}"


def serialize_route_choice(route_choice):
    return {
        "name": route_choice.name,
        # "function_call": route_choice.function_call,
        "similarity_score": route_choice.similarity_score
    }

def determine_intent(subqueries):
    for subquery in subqueries:
        task = subquery['subquery']
        intent = rl.retrieve_multiple_routes(task)
        # logging.info("intent:", intent)
        max_intent = max(intent, key=lambda choice: choice.similarity_score)

        # subquery['intent'] = [serialize_route_choice(choice) for choice in intent]
        serialized_intent = serialize_route_choice(max_intent)
        subquery['intent'] = serialized_intent  
        # subquery['intent'] = [serialize_route_choice(choice) for choice in intent]
    # logging.info(f"FINAL RESULT: {subqueries}")
    return subqueries

def route_query(intentions):
    topic = "intention_pipeline_topic"
    create_topic(topic)
    ensure_topic_exists(topic)
    
    final_response = None

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
            "response": response
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
        else:
            # The final response after all agents have processed
            final_response = message

    return final_response

@app.route('/intention_engine', methods=['POST'])
def handle_query():
    data = request.json
    query = data.get('query', '')
    logging.info(f"Query received: {query}\n")
    if not query:
        return jsonify({"error": "No query provided"}), 400

    try:
        subqueries = get_dag(query)
        logging.info(f"DAG: {subqueries}")
        intent_results = determine_intent(subqueries)
        logging.info(f"Intent: {intent_results}")
        final_response = route_query(intent_results)
        logging.info(f"Final response: {final_response}")
        return jsonify({"final_response": final_response}), 200        
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5003)
