from neo4j import Driver
from graph_creation.cwl_parsing import get_cwl_from_repo
from graph_creation.cwl_processing import process_cwl_inputs, process_cwl_outputs, process_cwl_steps
from neo4j_queries.node_queries import ensure_component_node

def process_repos(repo_list: list[str], driver: Driver) -> None:
    cwl_entities = {}
    for repo in repo_list:
        cwl_entities[repo]= get_cwl_from_repo(repo)
        for entity in cwl_entities[repo]:
            # if a component node with the same path as c does not exist then
            # create component node c_node unique to c with id equal to path and alias equal to a empty dictionary
            component_id = entity['path']
            ensure_component_node(driver, component_id)
            process_cwl_inputs(driver, entity)
            process_cwl_outputs(driver, entity)
            if entity['class'] == 'Workflow':
                process_cwl_steps(driver, entity, repo)
