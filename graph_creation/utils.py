from pathlib import Path
from neo4j import Driver
import re
from graph_creation.cwl_parsing import get_cwl_from_repo
from neo4j_dependency_queries.create_node_queries import ensure_component_node, ensure_in_parameter_node, ensure_out_parameter_node
from neo4j_dependency_queries.create_edge_queries import create_control_relationship, create_data_relationship, create_in_param_relationship
from neo4j_dependency_queries.processing_queries import get_all_in_parameter_nodes_of_entity

GITLAB_ASTRON ='https://git.astron.nl'

def process_step_lookup(cwl_entity: dict) -> dict[str, str]:
    """
    Processes the steps in a CWL entity to create a lookup dictionary mapping step IDs to their resolved file paths.

    Parameters:
        cwl_entity (dict): A dictionary representing a CWL entity, which includes a 'steps' key containing
                           the steps of the workflow and a 'path' key with the path to the workflow file

    Returns:
        dict: A dictionary where each key is the ID of the step in the context of the workflow, and the value is the resolved file path of the step
    """
    step_lookup = {}
    for step in cwl_entity['steps']:
        # Retrieve the directory containing the workflow file
        workflow_folder = Path(cwl_entity['path']).parent
        # Resolve the full path of the step file by combining the workflow folder and the step's 'run' path
        full_step_path = workflow_folder / Path(step['run'])
        # Resolve the path (deal with "./" and "../")
        step_path = str(resolve_relative_path(full_step_path))
        step_lookup[step['id']] = step_path
    return step_lookup

def process_in_param(driver: Driver, param_id: str, component_id: str, param_type: str, component_type: str) -> None:
    """
    Processes an input parameter by ensuring its node exists and optionally creating a relationship 
    between the component and the parameter node.

    Parameters:
        driver: The database or graph driver used to execute queries
        param_id (str): The unique identifier of the input parameter
        component_id (str): The ID of the component to which the parameter belongs
        is_workflow (bool): Indicates if the component is a workflow. If True, no relationship is created

    Returns:
        None
    """

    param_node = ensure_in_parameter_node(driver, param_id, component_id, param_type, component_type)
    if component_type != "Workflow":
        ensure_component_node(driver, component_id)
        create_in_param_relationship(driver, component_id, param_node[0])

def process_parameter_source(driver: Driver, param_node_internal_id: int, source_id: str, workflow_id: str, step_lookup: dict, step_id: str = "") -> None:
    """
    Processes a parameter source by creating a data relationship between a parameter node and its source.

    Parameters:
        driver (Driver): The Neo4j driver used to execute queries
        param_node_internal_id (int): The internal ID of the parameter node to which the relationship is being created
        source_id (str): The source identifier, which can be a single identifier (in case the source is an in-param of the workflow)
            or include a subcomponent (e.g., "source" or "sub_component/source")
        workflow_id (str): The ID of workflow to which the data relationships belong
        step_lookup (dict): A mapping of step identifiers to their respective unique paths

    Returns:
        None
    """
    source_param_node = get_source_node(driver, source_id, workflow_id, step_lookup)

    # Create a relationship between the parameter node and its source
    create_data_relationship(driver, source_param_node, param_node_internal_id,workflow_id, source_id, step_id)


def get_source_node(driver: Driver, source_id: str, workflow_id: str, step_lookup: dict) -> int:
    """
    Retrieves the node corresponding to the given source identifier.

    Parameters:
        driver (Driver): The Neo4j driver used to execute queries.
        source_id (str): The source identifier, which can be a single identifier (if the source is an in-param of the workflow component)
            or include a subcomponent (e.g., "source" or "sub_component/source").
            The second option can only be the case if the component is a workflow
        workflow_id (str): The ID of the workflow owning the data source.
        step_lookup (dict): A mapping of subcomponent identifiers to their respective unique paths within the workflow that is being analyzed.

    Returns:
        The internal ID of the source parameter node.
    """
    # Parse the source_id to identify whether it refers to a workflow parameter or an output of a subcomponent (subcomponent/id)
    source_parsed = source_id.split("/")
    source_param_node = None
    if len(source_parsed) == 1:
        # Ensure the source exists in the parameter node and retrieve it
        source_param_node = ensure_in_parameter_node(driver, source_parsed[0], workflow_id)[0]

    else:
        # If source_id refers to an output subcomponent/source
        # Retrieve the subcomponent ID from the step_lookup dictionary
        sub_component_id = step_lookup[source_parsed[0]]
        # Ensure the source exists in the output parameter node for the subcomponent
        source_param_node = ensure_out_parameter_node(driver, source_parsed[1], sub_component_id)[0]
    return source_param_node

def process_control_dependencies(driver: Driver, source_id: str, workflow_id: str, component_id: str, step_lookup: dict, step_id: str) -> None:
    """
    Processes control dependencies by creating control relationships between the given source and
    all in-parameters of the specified component.

    Parameters:
        driver (Driver): The Neo4j driver used to execute queries.
        source_id (str): The source identifier, which can be a single identifier or a subcomponent reference.
        workflow_id (str): The ID of the workflow to which the dependencies belong.
        component_id (str): The ID of the component owning the in-parameters.
        step_lookup (dict): A mapping of subcomponent identifiers to their respective unique paths within the workflow.

    Returns:
        None
    """
    source_param_node = get_source_node(driver, source_id, workflow_id, step_lookup)
    with driver.session() as session:
        in_parameters = get_all_in_parameter_nodes_of_entity(session, component_id)
        node_ids = [record["nodeId"] for record in in_parameters]
        for node_id in node_ids:
            create_control_relationship(driver, node_id, source_param_node, workflow_id, source_id, step_id)
        

def resolve_relative_path(path: Path)-> Path:
    """
    Resolves a relative path by simplifying `.` (current directory) 
    and `..` (parent directory) components without converting it to an absolute path.

    Parameters:
        path (Path): the input Path object to be resolved

    Returns:
        Path: a new object representing the simplified relative path

    Example:
        >>> resolve_relative_path(Path("x/y/../z"))
        Path('x/z')

        >>> resolve_relative_path(Path("./a/./b/c/../d"))
        Path('a/b/d')
    """
    parts = []
    for part in path.parts:
        if part == "..":
            if parts:
                parts.pop()
        elif part != ".":
            parts.append(part)
    return Path(*parts)

def extract_js_expression_dependencies(js_expression: str) -> list[tuple[str, str]]:
    """
    Extracts dependencies from a JavaScript expression in a CWL workflow step's "when" field.

    Parameters:
        js_expression (str): The JavaScript expression as a string.

    Returns:
        A list of references to step inputs or outputs.
            - ("parameter", [param ID]) for step inputs (e.g., inputs.myParamId)
            - ("step_output", [step ID]/[output ID]) for step outputs (e.g., steps.stepId.outputs.outID)
    """
    ref_list = []

    # Match inputs.[param ID]
    param_matches = re.findall(r'\binputs\.([a-zA-Z_]\w*)', js_expression)
    ref_list.extend([("parameter", param) for param in param_matches])

    # Match steps.[step ID].outputs.[output ID]
    step_output_matches = re.findall(r'\bsteps\.([a-zA-Z_]\w*)\.outputs\.([a-zA-Z_]\w*)', js_expression)
    ref_list.extend([("step_output", f"{step}/{output}") for step, output in step_output_matches])

    return ref_list

def get_input_source(inputs: list[dict], input_id: str) -> str | None:
    """
    Retrieves the 'source' value for a given input ID from a list of input dictionaries.

    Parameters:
        inputs (list[dict]): A list of dictionaries representing CWL step inputs.
        input_id (str): The ID of the input to search for.

    Returns:
        The value associated with the 'source' key if found, otherwise None.
    """
    for inp in inputs:
        if inp["id"] == input_id:
            return inp.get("source")  # returns None if 'source' doesn't exist
    return None  # returns None if input_id is not found
