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
from process_gitlab.process_repos import clone_repos, save_commit_history_for_evaluation, delete_cloned_repos
from neo4j_graph_queries.delete_existing_graph import delete_graph
from neo4j.exceptions import Neo4jError



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
    clone_repos(relevant_repos, folder)
    repo_paths = [Path(folder).joinpath(repo) for repo in relevant_repos]

    # Get the authentication details for Neo4j instance
    #**SANGEETH: WHEN CONVERTING TO A PROPER SERVER BACKEND, ENSURE THAT FOR EVERY RUN, A NEW NEO4J INSTANCE IS CREATED

    # Dynamically read configurations from Docker environment variables
    # If variables aren't found, it safely falls back to standard local defaults - these are local defaults and not hardcoded credentials.
    URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user_name = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "your_new_password")
    
    AUTH = (user_name, password)
    
    print(f"Connecting to Neo4j instance at: {URI} using username: {user_name}")

    try
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
            print("Connection established.")
            driver = GraphDatabase.driver(URI, auth=AUTH)
            delete_graph(driver)
            process_repos(repo_paths, driver)
            neo4j_traversal = SubgraphPreprocessing(driver)
            neo4j_traversal.preprocess_all_graphs()

            pprint(get_graph_size_per_repo(driver.session(), relevant_repos))

            #The following is for analysis of flow paths and change impact. However, it is commented out for now as it is not needed for creation of the graph and interacting with it.
            #analysis(driver, relevant_repos)
            
            #Count the total number of CWL files across all repositories
            total = 0
            for path in repo_paths:
                pathlist = list(Path(path).rglob("*.cwl"))
                print(f'{path} has {len(pathlist)}')
                total += len(pathlist)
            print(f"Total CWL files: {total}")

    except Neo4jError as neo4j_err:
        print(f"Neo4j Database error encountered: {neo4j_err}")
    except Exception as err:
        print(f"An unexpected error occurred during processing: {err}")
    finally:
        # Safely close the driver if it was successfully instantiated
        if 'driver' in locals() and driver:
            driver.close()
            print("Driver connection safely closed.")
            driver.close()
    
    #cleanup: Delete the cloned repositories after processing to free up space
    delete_cloned_repos(folder)
    
    

def analysis(driver, relevant_repos):
    flow_calculation = FlowCalculation(driver)
    flow_calculation.perform_flow_path_calculation()

    with open("flow_paths.json", "r") as json_file:
        paths = json.load(json_file)
    change_impact = ChangeImpact(driver)
    change_impact.complete_path_analysis(paths)
    # save_commit_history_for_evaluation()
    with open("commits_for_evaluation.json", "r") as json_file:
        commit_history = json.load(json_file)
    calculate_co_change_ratios(commit_history)

    evaluate_coupling("change_impact_analysis.csv","history_percent.csv")
    change_impact.change_impact_exploration("change_impact_analysis.csv", relevant_repos)
