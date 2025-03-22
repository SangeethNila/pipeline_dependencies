import json
from pathlib import Path
from graph_analysis.metric_calculations.FlowCalculation import FlowCalculation
from graph_analysis.metric_calculations.ChangeImpact import ChangeImpact
from graph_analysis.subgraph_preprocessing.SubgraphPreprocessing import SubgraphPreprocessing
from graph_analysis.general_analysis import get_graph_size_per_repo
from metric_evaluation.change_impact_eval import evaluate_coupling
from neo4j_graph_queries.utils import clean_component_id
from process_gitlab.process_history import  calculate_co_change_ratios
from graph_creation.repo_processing import process_repos
from neo4j import GraphDatabase
import dotenv
import os
import pandas as pd
from pprint import pprint
from process_gitlab.process_repos import clone_repos, save_commit_history_for_evaluation



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

    # Get the authentication details for Neo4j instance
    load_status = dotenv.load_dotenv("Neo4j-25ebc0db-Created-2024-11-17.txt")
    if load_status is False:
        raise RuntimeError('Environment variables not loaded.')

    URI = os.getenv("NEO4J_URI")
    AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

    repo_paths = [f'{folder}\\{Path(path)}' for path in relevant_repos]
    
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print("Connection established.")
        driver = GraphDatabase.driver(URI, auth=AUTH)
        # process_repos(repo_paths, driver)
        # neo4j_traversal = SubgraphPreprocessing(driver)
        # neo4j_traversal.preprocess_all_graphs()

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

