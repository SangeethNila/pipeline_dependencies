from neo4j import Driver 
from graph_creation.cwl_parsing import get_cwl_from_repo
from graph_creation.docker_parsing import parse_all_dockerfiles
from graph_creation.utils import process_step_lookup
from graph_creation.cwl_processing import process_cwl_commandline, process_cwl_inputs, process_cwl_outputs, process_cwl_steps
from neo4j_graph_queries.utils import get_is_workflow
import pprint

def process_repos(repo_list: list[str], driver: Driver) -> None:
    """
    Processes a list of local repository paths containing CWL (Common Workflow Language) files,
    parsing each CWL file and creating the corresponding nodes and relationships in a Neo4j graph.

    The function extracts workflows and tools from each repository, processes the inputs, outputs, and 
    steps for each entity, and links them into a dependency graph. The Neo4j driver is used to interact 
    with the database, creating nodes and relationships based on the parsed CWL data.

    Parameters:
        repo_list (list[str]): A list of paths to local repositories. Each repository contains CWL files 
                               that define workflows and tools
        driver (Driver): A Neo4j driver used to interact with the database

    Returns:
        None
    """
    for repo in repo_list:
        # Parse CWL files of current repo
        all_entities = get_cwl_from_repo(repo)

        # links = parse_all_dockerfiles(repo)
        for entity in all_entities:
            print(f'Processing: {entity["path"]}')
            is_workflow = get_is_workflow(entity)
            steps = None
            if is_workflow:
                steps = process_step_lookup(entity)
            process_cwl_inputs(driver, entity)
            process_cwl_outputs(driver, entity, steps)
            if steps:
                process_cwl_steps(driver, entity, steps)
            # elif entity['class'] == 'ExpressionTool':
            #     process_cwl_expression(driver, entity)
            # elif entity['class'] == 'CommandLineTool':
            #     process_cwl_commandline(driver, entity, links)