import json
from pathlib import Path
from graph_analysis.metric_calculations.FlowCalculation import FlowCalculation
from graph_analysis.metric_calculations.ChangeImpact import ChangeImpact
from graph_analysis.subgraph_preprocessing.SubgraphPreprocessing import SubgraphPreprocessing
from graph_analysis.general_analysis import get_graph_size_per_repo
from metric_evaluation.change_impact_eval import evaluate_coupling
from neo4j_graph_queries.utils import clean_component_id
#from process_gitlab.process_history import  calculate_co_change_ratios
#Sangeeth: The above one is creating eroor - the python module was unable to be installed
from graph_creation.repo_processing import process_repos
from neo4j import GraphDatabase
import dotenv
import os
import pandas as pd
from pprint import pprint
from process_gitlab.process_repos import clone_repos, save_commit_history_for_evaluation
#sangeeth: written the below code:
from neo4j_graph_queries.delete_existing_graph import delete_graph



if __name__ == '__main__':
    relevant_repos = [
        'ldv/imaging_compress_pipeline', 
        'RD/LINC',
        # 'RD/rapthor',
        'RD/VLBI-cwl',
        'RD/preprocessing-cwl',
        'ssw-ksp/solar-bf-compressing',
        'ldv/bf_double_tgz'
    ]
    folder = 'repos'
    # clone_repos(relevant_repos, folder)
    #Sangeeth: Find an elegant way to stop cloning everytime - The above line clones repos everyrun. However we need it for only the first run. After that comment it out.


    # repo_paths = [f'{folder}\\{Path(path)}' for path in relevant_repos]
    #Sangeeth: I am assuming that Chiara used string concatenation to combine folder name to the rest of the path, however this is not portable to other OSs.
    #thus correcting it to use Path.joinpath() method.
    repo_paths = [Path(folder).joinpath(repo) for repo in relevant_repos]
    # print(f"repo_paths: {[str(path) for path in repo_paths]}")
    # exit(0)  # Sangeeth: Added to stop execution for debugging


    # Get the authentication details for Neo4j instance
    #Sangeeth: The current one given is my neo4j details -- please replace it with your own details in the .env file; do not share your credentials with anyone.
    #Sangeeth: The following is commented out to move from Neo4j Aura to a local instance.
    # load_status = dotenv.load_dotenv("Neo4j-b457d5b3-Created-2025-08-19.txt")
    # if load_status is False:
    #     print("Environment variables not loaded from file")
    #     # raise RuntimeError('Environment variables not loaded.')
    # URI = os.getenv("NEO4J_URI")
    # AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

    #SANGEETH: WHEN CONVERTING TO A PROPER SERVER BACKEND, ENSURE THAT FOR EVERY RUN, A NEW NEO4J INSTANCE IS CREATED

    # #URI = "bolt://localhost:7687"
    # URI = "bolt://localhost:7474"
    # user_name = "neo4j"
    # password = "your_new_password"  # Change this to your new password if you have
    # AUTH = (user_name, password)


    # 1. Dynamically read configurations from Docker environment variables
    # If variables aren't found, it safely falls back to standard local defaults.
    URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user_name = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "your_new_password")
    
    AUTH = (user_name, password)
    
    print(f"Connecting to Neo4j instance at: {URI} using username: {user_name}")


    #Changing default password
    # new_password = "your_new_password"

    # with GraphDatabase.driver(URI, auth=AUTH) as driver:
    #     with driver.session(database="system") as session:
    #         session.run(
    #             "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $new_password",
    #             new_password=new_password
    #         )
    # print("Password changed successfully.")
    # AUTH = ("neo4j", new_password) 



    
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print("Connection established.")
        driver = GraphDatabase.driver(URI, auth=AUTH)
        delete_graph(driver)
        process_repos(repo_paths, driver)
        neo4j_traversal = SubgraphPreprocessing(driver)
        neo4j_traversal.preprocess_all_graphs()

        pprint(get_graph_size_per_repo(driver.session(), relevant_repos))

        # flow_calculation = FlowCalculation(driver)
        # flow_calculation.perform_flow_path_calculation()

        # with open("flow_paths.json", "r") as json_file:
        #     paths = json.load(json_file)
        # change_impact = ChangeImpact(driver)
        # change_impact.complete_path_analysis(paths)
        # # save_commit_history_for_evaluation()
        # with open("commits_for_evaluation.json", "r") as json_file:
        #     commit_history = json.load(json_file)
        # calculate_co_change_ratios(commit_history)

        # evaluate_coupling("change_impact_analysis.csv","history_percent.csv")
        # change_impact.change_impact_exploration("change_impact_analysis.csv", relevant_repos)
    

        total = 0
        for path in repo_paths:
            pathlist = list(Path(path).rglob("*.cwl"))
            print(f'{path} has {len(pathlist)}')
            total += len(pathlist)
        print(total)


        driver.close()

