from neo4j import Driver 
from process_history.process_history import get_cwl_change_history
from metric_calculations.CalculationComponentAnalyzer import CalculationComponentAnalyzer
from metric_calculations.Neo4jTraversalDFS import Neo4jTraversalDFS
from graph_creation.cwl_parsing import get_cwl_from_repo
from graph_creation.docker_parsing import parse_all_dockerfiles
from graph_creation.utils import process_step_lookup
from graph_creation.cwl_processing import process_cwl_commandline, process_cwl_inputs, process_cwl_outputs, process_cwl_steps
from neo4j_queries.edge_queries import clean_relationship
from neo4j_queries.node_queries import ensure_component_node
from neo4j_queries.utils import clean_component_id, get_is_workflow

def process_repos(repo_list: list[str], driver: Driver, build = True, calculate = False) -> None:
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
        workflows, tools = get_cwl_from_repo(repo)
        # Extract tool paths for step processing later
        tool_paths = [item["path"] for item in tools]
        # Combine workflows and tools into one list of entities to process
        all_entities = workflows + tools

        if build:
            # links = parse_all_dockerfiles(repo)
            clean_relationship(driver)

            for entity in all_entities:
                print(f'Processing: {entity["path"]}')
                is_workflow = get_is_workflow(entity)
                steps = None
                if not is_workflow:
                    ensure_component_node(driver, entity['path'])
                else:
                    steps = process_step_lookup(entity)
                process_cwl_inputs(driver, entity)
                process_cwl_outputs(driver, entity, steps)
                if steps:
                    process_cwl_steps(driver, entity, tool_paths, steps)
                # elif entity['class'] == 'ExpressionTool':
                #     process_cwl_expression(driver, entity)
                # elif entity['class'] == 'CommandLineTool':
                #     process_cwl_commandline(driver, entity, links)
        if calculate:
            processed_entities = set()
            neo4j_traversal = Neo4jTraversalDFS(driver)
            for entity in all_entities:
                print(f'Processing: {entity["path"]}')
                is_workflow = get_is_workflow(entity)
                if is_workflow:
                    if clean_component_id(entity['path']) not in processed_entities:
                        new_entities = neo4j_traversal.traverse_subgraph(entity['path'], entity['class'])
                        processed_entities = processed_entities.union(new_entities)
