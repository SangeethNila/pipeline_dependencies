from pathlib import Path
import re
from neo4j import Driver
from graph_creation.utils import extract_js_expression_dependencies, get_input_source, process_control_dependencies, process_in_param, process_parameter_source
from neo4j_dependency_queries.create_node_queries import ensure_git_node, ensure_in_parameter_node, ensure_out_parameter_node
from neo4j_dependency_queries.create_edge_queries import  create_out_param_relationship, create_references_relationship
from neo4j_dependency_queries.utils import get_is_workflow

def process_cwl_inputs(driver: Driver, cwl_entity: dict) -> None:
    """
    Processes the inputs of a CWL  entity, either as a list or a dictionary of inputs,
    and processes each input parameter by calling `process_in_param`.

    Parameters:
        driver (Driver): The Neo4j driver used to execute queries.
        cwl_entity (dict): A dictionary representing a CWL entity, which includes an 'inputs' key containing
                           either a list or dictionary of input parameters.

    Returns:
        None
    """
    component_id = cwl_entity['path']
    # Process the inputs based on their type (list or dictionary)
    if isinstance(cwl_entity['inputs'], list):
        # If 'inputs' is a list, iterate over each input (which is expected to be a dictionary)
        for input in cwl_entity['inputs']:
            if isinstance(input, dict):
                process_in_param(driver, input['id'], component_id, input['type'], cwl_entity['class'])
    elif isinstance(cwl_entity['inputs'], dict):
        # If 'inputs' is a dictionary, iterate over the keys (which are the input IDs)
        input_dict = cwl_entity['inputs']
        for key in input_dict.keys():
            process_in_param(driver, key, component_id, input_dict[key]['type'], cwl_entity['class'])

def process_cwl_outputs(driver: Driver, cwl_entity: dict, step_lookup: dict) -> None:
    """
    Processes the output parameters of a CWL entity by creating the necessary nodes 
    and relationships for each output parameter in a graph or database. The function handles both singular and 
    list-based output sources, ensuring that each output is linked to its corresponding source or sources.

    For each output in the CWL entity:
        - An out-parameter node is created for the output.
        - If the output contains an 'outputSource', the function processes the relationship between the output 
          parameter and its source(s). The 'outputSource' can either be a single source ID or a list of source IDs.

    Parameters:
        driver (Driver): The Neo4j driver used to execute queries
        cwl_entity (dict): A dictionary representing a CWL entity, which includes:
            - 'path' (str): The path to the CWL file, used as the component ID.
            - 'outputs' (list): A list of output parameters. Each output is a dictionary containing:
                - 'id' (str): The unique identifier of the output parameter.
                - 'outputSource' (str or list of str): The source(s) for the output parameter, which can be a single
                  source ID or a list of source IDs.
        step_lookup (dict): A dictionary that maps step IDs to their corresponding resolved paths. This is used to 
                             resolve the source ID(s) in the 'outputSource' field to their correct locations.

    Returns:
        None
    """
    component_id = cwl_entity['path']
    for output in cwl_entity['outputs']:
        if isinstance(output, dict):
            # Create out-parameter node with the parameter ID as defined in the component
            # and component ID equal to the path of the componet
            out_param_node = ensure_out_parameter_node(driver, output['id'], component_id, output["type"], cwl_entity['class'])
            out_param_node_internal_id = out_param_node[0]

            # If it's not a workflow, create a relationship between the component and the output parameter
            is_worflow = get_is_workflow(cwl_entity)
            if not is_worflow:
                create_out_param_relationship(driver, component_id, out_param_node_internal_id, output['id'])

            # If the output has an 'outputSource', process the relationship(s) to the source(s)
            if 'outputSource' in output:
                # The output source can be a singular ID or a list of IDs
                if isinstance(output['outputSource'], str):
                    source_id = output['outputSource']
                    process_parameter_source(driver, out_param_node_internal_id, source_id, component_id, step_lookup)
                elif isinstance(output['outputSource'], list):
                    for source_id in output['outputSource']:
                        process_parameter_source(driver, out_param_node_internal_id, source_id, component_id, step_lookup)
   
def process_cwl_steps(driver: Driver, cwl_entity: dict, step_lookup) -> None:   
    """
    Processes the steps of a CWL entity, creating necessary nodes and relationships 
    for each step. The function handles the inputs, outputs, and control dependencies associated with each step 
    in the workflow

    For each step in the CWL entity:
        - The inputs are processed by creating in-parameter nodes and establishing relationships with the step
        - The "when" field (control dependencies) is processed by extracting the dependent parameters or outputs 
          and creating control relationships
=
    Parameters:
        driver (Driver): The Neo4j driver used to execute queries
        cwl_entity (dict): A dictionary representing a CWL entity, which includes:
            - 'path' (str): The path to the CWL file, used as the component ID.
            - 'steps' (list): A list of steps in the workflow, each step being a dictionary containing:
                - 'id' (str): The unique identifier for the step.
                - 'in' (list): A list of inputs for the step.
                - 'when' (str, optional): A conditional expression controlling the execution of the step.
        step_lookup (dict): A dictionary that maps step IDs to their resolved paths. This is used to resolve 
                             the actual paths of steps when processing their inputs, outputs, and control dependencies.

    Returns:
        None
    """ 
    workflow_id = cwl_entity['path']

    for step in cwl_entity['steps']:

        # Get the resolved path of the step from the step_lookup
        step_path = step_lookup[step['id']]

        # Process the list of inputs of the step 
        for input in step['in']:
            # Create in-parameter node with ID as defined in the component and component ID equal to the path of the step
            param_node = ensure_in_parameter_node(driver, input['id'], step_path)
            param_node_internal_id = param_node[0]

            # Inputs can have one or multiple data sources (data nodes)
            if 'source' in input:
                if isinstance(input['source'], str):
                    source_id = input['source']
                    process_parameter_source(driver, param_node_internal_id, source_id, workflow_id, step_lookup, step['id'])
                elif isinstance(input['source'], list):
                    for source_id in input['source']:
                        process_parameter_source(driver, param_node_internal_id, source_id, workflow_id, step_lookup, step['id'])

        # Process the "when" field, aka control dependencies
        if 'when' in step:
            when_expr = step['when']
            # Exact parameter references within conditional
            when_refs = extract_js_expression_dependencies(when_expr)
            source = None
            for ref in when_refs:
                if ref[0] == "parameter":
                    # Retrieve the source of the referenced input parameter
                    source = get_input_source(step['in'], ref[1])
                else: 
                    # The reference already mentions the source (output of a step)
                    source = ref[1]
                if source:
                    # Create control dependencies from the in-parameters of the step to the source of the reference
                    if isinstance(source, list):
                        # If the source is a list, process each source ID individually
                        for source_id in source:
                            process_control_dependencies(driver, source_id, workflow_id, step_path, step_lookup, step['id'])
                    else:
                        # Process the single source dependency
                        process_control_dependencies(driver, source, workflow_id, step_path, step_lookup, step['id'])


def process_cwl_base_commands(driver: Driver, entity: dict, links: dict[str, dict]):
    """
    Processes the 'baseCommand' field in a CWL entity, with the aim of creating relationships 
    to external GitLab in the graph.

    Parameters:
        driver: The Neo4j driver for executing queries.
        entity (dict): The CWL entity containing the 'baseCommand' field.
        links (dict): A dictionary containing:
            - "commands": Mapping of command names (executables) to external Git repository they belong to.
            - "paths": Mapping of file paths to external Git repository they originate from.

    Returns:
        list or None: A list of command strings if 'baseCommand' exists, otherwise None.

    Process:
        - Extracts the 'baseCommand' from the entity.
        - If it's a list, retrieves the first command and joins all commands into a string.
        - Checks if the first command has a file extension:
            - If yes, searches for a matching executable in 'link_paths'.
            - If no, checks if it's in 'link_commands'.
        - If a match is found, ensures the command's Git node exists and 
          creates a reference relationship in the database.
    """
    base_command_key = "baseCommand"
    link_commands = links["commands"]
    link_paths = links["paths"]
    if base_command_key in entity:
        commands = entity[base_command_key]
        if isinstance(commands, list):
            if commands:
                first_command = commands[0]
                all_commands = " ".join(commands)
                extension = Path(first_command).suffix
                if extension:
                    # If the first command has a file extension, look for an executable match
                    for key, value in link_paths.items():
                        if is_executable(key) and first_command in key:
                            git_internal_node_id = ensure_git_node(driver, value)[0]
                            create_references_relationship(driver, entity["path"], git_internal_node_id, all_commands)
                            break
                else:
                    # If no extension, check if the first command exists in link_commands
                    if first_command in link_commands:
                        git_internal_node_id = ensure_git_node(driver, link_commands[first_command])[0]
                        create_references_relationship(driver, entity["path"], git_internal_node_id, all_commands)
        return commands
    return None

def is_executable(path):
    prefix = Path("\\usr\\local\\bin")
    return Path(path).parts[:len(prefix.parts)] == prefix.parts

def get_executable(path):
    prefix = Path("\\usr\\local\\bin")
    if Path(path).parts[:len(prefix.parts)] == prefix.parts:
        return Path(*Path(path).parts[len(prefix.parts):])

def process_cwl_commandline(driver: Driver, entity: dict, links: dict[str, dict]) -> None:
    """
    Processes the command-line tool CWL entity by resolving 
    dependencies and linking them to relevant Git repository nodes.

    Parameters:
        driver: The Noe4j driver for executing queries.
        entity (dict): The CWL entity containing command-line tool definitions.
        links (dict): A dictionary containing:
            - "commands": Mapping of command names to file paths.
            - "paths": Mapping of file paths to Git repository locations.

    Process:
        1. Calls `process_cwl_base_commands()` to extract the command list.
        2. Extracts file listings from 'InitialWorkDirRequirement' in 'requirements'.
        3. If both commands and listings exist:
            - Maps entry names to their content.
            - Iterates over commands, checking for references in the listing.
            - If a match is found in either 'commands' or 'paths', creates a 
              reference relationship in the database.
            - If the command is executable, checks for references using the 
              executable's path as well.

    Returns:
        None
    """
    commands = process_cwl_base_commands(driver, entity, links)
    listing = None
    if "requirements" in entity:
        if "InitialWorkDirRequirement" in entity["requirements"]:
            listing = entity["requirements"]["InitialWorkDirRequirement"]["listing"]
        else:
            init_work_dir = next((item for item in entity["requirements"] if item.get('class') == 'InitialWorkDirRequirement'), None)
            if init_work_dir:
                listing = init_work_dir["listing"]
    if commands and listing:
        entry_map = {entry["entryname"]: entry["entry"] for entry in listing if "entryname" in entry}
        for command in commands:
            if command in entry_map:
                all_links = links["commands"] | links["paths"]
                print(entry_map[command])

                for key, value in all_links.items():
                    path = str(Path(key).as_posix())
                    if bool(re.search(rf'\b{re.escape(path)}\b', entry_map[command])):
                        print(f"created: {value}")
                        git_internal_node_id = ensure_git_node(driver, value)[0]
                        create_references_relationship(driver, entity["path"], git_internal_node_id, entry_map[command])
                    if is_executable(key):
                        executable = str(get_executable(key))
                        if bool(re.search(rf'\b{re.escape(executable)}\b', entry_map[command])):
                            print(f"created: {value}")
                            git_internal_node_id = ensure_git_node(driver, value)[0]
                            create_references_relationship(driver, entity["path"], git_internal_node_id, entry_map[command])


