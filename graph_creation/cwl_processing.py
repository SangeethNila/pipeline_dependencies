from pathlib import Path
import re
from neo4j import Driver
from graph_creation.cst_processing import traverse_when_statement_extract_dependencies
from graph_creation.utils import process_in_param, process_parameter_source
from neo4j_queries.node_queries import ensure_component_node, ensure_git_node, ensure_in_parameter_node, ensure_out_parameter_node
from neo4j_queries.edge_queries import create_control_relationship, create_data_relationship, create_out_param_relationship, create_references_relationship

from neo4j_queries.utils import get_is_workflow
from parsers.javascript_parsing import parse_javascript_expression_string, parse_javascript_string


# TODO: deal with inputBindings
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
    is_workflow = get_is_workflow(cwl_entity)
    # Process the inputs based on their type (list or dictionary)
    if isinstance(cwl_entity['inputs'], list):
        # If 'inputs' is a list, iterate over each input (which is expected to be a dictionary)
        for input in cwl_entity['inputs']:
            if isinstance(input, dict):
                process_in_param(driver, input['id'], component_id, is_workflow, input['type'])
    elif isinstance(cwl_entity['inputs'], dict):
        # If 'inputs' is a dictionary, iterate over the keys (which are the input IDs)
        for key in cwl_entity['inputs'].keys():
            process_in_param(driver, key, component_id, is_workflow)

# TODO: deal with outputBindings
def process_cwl_outputs(driver: Driver, cwl_entity: dict, step_lookup) -> None:
    """
    Processes the output parameters of a CWL entity by creating the necessary nodes 
    and relationships for each output parameter in a graph or database. The function handles both singular and 
    list-based output sources, ensuring that each output is linked to its corresponding source or sources.

    For each output in the CWL entity:
        - An out-parameter node is created for the output.
        - If the CWL entity is not a workflow, a relationship is created between the component node and the output parameter.
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
            out_param_node = ensure_out_parameter_node(driver, output['id'], component_id, output["type"])
            out_param_node_internal_id = out_param_node[0]

            # If it's not a workflow, create a relationship between the component and the output parameter
            is_worflow = get_is_workflow(cwl_entity)
            if not is_worflow:
                create_out_param_relationship(driver, component_id, out_param_node_internal_id)

            # If the output has an 'outputSource', process the relationship(s) to the source(s)
            if 'outputSource' in output:
                # The output source can be a singular ID or a list of IDs
                if isinstance(output['outputSource'], str):
                    source_id = output['outputSource']
                    process_parameter_source(driver, out_param_node_internal_id, source_id, component_id, step_lookup)
                elif isinstance(output['outputSource'], list):
                    for source_id in output['outputSource']:
                        process_parameter_source(driver, out_param_node_internal_id, source_id, component_id, step_lookup)
   
def process_cwl_steps(driver: Driver, cwl_entity: dict, tool_paths: list[str], step_lookup) -> None:   
    """
    Processes the steps of a CWL entity, creating necessary nodes and relationships 
    for each step. The function handles the inputs, outputs, and control dependencies associated with each step 
    in the workflow

    For each step in the CWL entity:
        - A component node is created for the step if it corresponds to a tool (identified via tool_paths)
        - The inputs are processed by creating in-parameter nodes and establishing relationships with the step
        - The "when" field (control dependencies) is processed by extracting the dependent parameters or outputs 
          and creating control relationships
        - The outputs are processed by creating out-parameter nodes and establishing relationships with the step

    Parameters:
        driver (Driver): The Neo4j driver used to execute queries
        cwl_entity (dict): A dictionary representing a CWL entity, which includes:
            - 'path' (str): The path to the CWL file, used as the component ID.
            - 'steps' (list): A list of steps in the workflow, each step being a dictionary containing:
                - 'id' (str): The unique identifier for the step.
                - 'in' (list): A list of inputs for the step.
                - 'out' (list): A list of outputs for the step.
                - 'when' (str, optional): A conditional expression controlling the execution of the step.
        tool_paths (list[str]): A list of paths that correspond to tool steps. These paths are used to determine 
                                whether a step corresponds to a tool or not.
        step_lookup (dict): A dictionary that maps step IDs to their resolved paths. This is used to resolve 
                             the actual paths of steps when processing their inputs, outputs, and control dependencies.

    Returns:
        None
    """ 
    component_id = cwl_entity['path']

    for step in cwl_entity['steps']:

        # Get the resolved path of the step from the step_lookup
        step_path = step_lookup[step['id']]

        is_tool = step_path in tool_paths

        # Create the step component node if it's a tool
        if step_path in tool_paths:
            is_tool = True
            s_node = ensure_component_node(driver, step_path)
            s_node_internal_id = s_node[0]

        # Process the list of inputs of the step 
        for input in step['in']:
            process_in_param(driver, input['id'], step_path, not is_tool)
            # Create in-parameter node with ID as defined in the component and component ID equal to the path of the step
            param_node = ensure_in_parameter_node(driver, input['id'], step_path)
            param_node_internal_id = param_node[0]
            if is_tool:
                # Create a data edge from the step component node to the in-parameter node
                create_data_relationship(driver, s_node_internal_id, param_node_internal_id, step_path, input['id'])

            # Inputs can have one or multiple data sources (data nodes)
            if 'source' in input:
                if isinstance(input['source'], str):
                    source_id = input['source']
                    process_parameter_source(driver, param_node_internal_id, source_id, component_id, step_lookup)
                elif isinstance(input['source'], list):
                    for source_id in input['source']:
                        process_parameter_source(driver, param_node_internal_id, source_id, component_id, step_lookup)

        # Process the "when" field, aka control dependencies
        if 'when' in step:
            when_expr = step['when']
            expr_tree = parse_javascript_expression_string(when_expr)
            when_refs = traverse_when_statement_extract_dependencies(expr_tree)

            nodes = []
            for ref in when_refs:
                ref_id = ref[1]
                if ref[0] == "parameter":
                    input_data = ensure_in_parameter_node(driver, ref_id, step_path)[0]
                    nodes.append(input_data)
                # elif ref[0] == "step_output":
                #     step_output = ensure_out_parameter_node(driver, ref_id, cwl_entity['path'])[0]
                #     nodes.append(step_output)

            for node in nodes:
                create_control_relationship(driver, s_node_internal_id, node, cwl_entity['path'])

        # Process the list of outputs of the step
        for output in step['out']:
            # An output can be defined as a dictionary or simply as a string (ID only)
            # Create out-parameter node with ID as defined in the component and component ID equal to the path of the step
            if isinstance(output, dict):
                output_id = output['id']
            else:
                output_id = output
            param_node = ensure_out_parameter_node(driver, output_id, step_path)
            param_node_internal_id = param_node[0]
            if is_tool:
                # Create a data edge from out-parameter node to the step component node
                create_data_relationship(driver, param_node_internal_id, s_node_internal_id, step_path, output_id)


def process_cwl_base_commands(driver, entity, links):
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
                    for key, value in link_paths.items():
                        if is_executable(key) and first_command in key:
                            git_internal_node_id = ensure_git_node(driver, value)[0]
                            create_references_relationship(driver, entity["path"], git_internal_node_id, all_commands)
                            break
                else:
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

def process_cwl_commandline(driver, entity, links):
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
                # Regular expression pattern to match POSIX file paths
                # pattern = r'(?<![\'"\[])(/[\w/.-]+(?:\.[a-zA-Z0-9]+)?)(?![\'"\]])'

                # # Find all POSIX paths in the bash script
                # posix_paths = re.findall(pattern, entry_map[command])
                # print(posix_paths)

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
    print()


