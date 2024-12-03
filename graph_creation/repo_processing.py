from neo4j import Driver
from graph_creation.cwl_parsing import get_cwl_from_repo
from graph_creation.cwl_processing import process_cwl_inputs, process_cwl_outputs, process_cwl_steps
from neo4j_queries.node_queries import ensure_component_node

def process_repos(repo_list: list[str], driver: Driver) -> None:
    """
    Given a list of paths to local repositories and a Neo4j driver,
    the function parses the CWL files and turns them into a Neo4j dependency graph.

    Parameters:
        repo_list (list[str]): a list of paths to local repositories
        driver (Driver): a Neo4j driver
    """
    cwl_entities = {}
    for repo in repo_list:
        # Parse CWL files
        cwl_entities[repo]= get_cwl_from_repo(repo)
        for entity in cwl_entities[repo]:
            component_id = entity['path']
            ensure_component_node(driver, component_id)
            process_cwl_inputs(driver, entity)
            process_cwl_outputs(driver, entity)
            if entity['class'] == 'Workflow':
                process_cwl_steps(driver, entity)
