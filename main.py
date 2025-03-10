from graph_traversal.metric_calculations.FlowCalculation import FlowCalculation
from graph_traversal.metric_calculations.ChangeImpact import ChangeImpact
from graph_traversal.subgraph_preprocessing.SubgraphPreprocessing import SubgraphPreprocessing
from metric_evaluation.change_impact_eval import evaluate_coupling
from neo4j_graph_queries.utils import clean_component_id
from process_gitlab.process_history import  calculate_co_change_ratios
from graph_creation.repo_processing import process_repos
from neo4j import GraphDatabase
import dotenv
import os
import pandas as pd

from process_gitlab.process_repos import clone_repos, save_commit_history_for_evaluation



if __name__ == '__main__':
    relevant_repos = [
        'ldv\\imaging_compress_pipeline'
                    , 
                    'RD\\LINC'
                      ]
    folder = 'repos'
    # clone_repos(relevant_repos, folder)

    # Get the authentication details for Neo4j instance
    load_status = dotenv.load_dotenv("Neo4j-25ebc0db-Created-2024-11-17.txt")
    if load_status is False:
        raise RuntimeError('Environment variables not loaded.')

    URI = os.getenv("NEO4J_URI")
    AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

    repo_paths = [f'{folder}\\{path}' for path in relevant_repos]
    
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print("Connection established.")
        driver = GraphDatabase.driver(URI, auth=AUTH)
        # clone_repos(relevant_repos, folder)
        # process_repos(repo_paths, driver)
        # neo4j_traversal = SubgraphPreprocessing(driver)
        # neo4j_traversal.preprocess_all_graphs()

        # flow_calculation = FlowCalculation(driver)
        # flow_calculation.perform_flow_path_calculation()

        # with open("flow_paths.json", "r") as json_file:
        #     paths = json.load(json_file)
        # change_impact = ChangeImpact(driver)
        # change_impact.complete_path_analysis(paths)
        # with open("commits_for_evaluation.json", "r") as json_file:
        #     commit_history = json.load(json_file)
        # calculate_co_change_ratios(commit_history)

        evaluate_coupling("change_impact_analysis.csv","history_percent.csv")

        driver.close()

