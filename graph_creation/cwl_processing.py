from neo4j import Driver
from graph_creation.cst_processing import traverse_when_statement_extract_dependencies
from graph_creation.utils import create_input_nodes_and_relationships, process_source_relationship, resolve_relative_path
from neo4j_queries.node_queries import ensure_component_node, ensure_data_node, ensure_parameter_node, get_wf_data_nodes_from_step_in_param
from neo4j_queries.edge_queries import create_control_relationship, create_data_relationship, create_out_param_relationship
from pathlib import Path

from parsers.javascript_parsing import parse_javascript_expression_string

# TODO: deal with inputBindings
def process_cwl_inputs(driver: Driver, cwl_entity: dict) -> None:
    """
    Processes the inputs of a CWL component (Workflow, CommandLineTool, or ExpressionTool)
    For each input the following nodes and edges are created:
    - an in-parameter node with the parameter ID as defined in the component and component ID equal to the path of the componet
    - a data node with component ID of the component and data ID equal to the parameter ID
    - a data edge from the component node to the in-parameter node
    - a data edge from the data node to the the in-parameter node

    Parameters:
        driver (Driver): the driver used to connect to Neo4j
        cwl_entity (dict): the dictionary containing the parsed contents of the CWL component
    """
    component_id = cwl_entity['path']
    # Inputs can be defined a list or a dictionary
    if type(cwl_entity['inputs']) == list:
        # List of dictionaries
        # each element is identifiable via the key 'id'
        for input in cwl_entity['inputs']:
            if type(input) == dict:
                create_input_nodes_and_relationships(driver, input['id'], component_id)
    elif type(cwl_entity['inputs']) == dict:
        # Dictionary where each key is the ID of the input
        # the value is a dictionary containing other properties
        for key in cwl_entity['inputs'].keys():
            create_input_nodes_and_relationships(driver, key, component_id)

# TODO: deal with outputBindings
def process_cwl_outputs(driver: Driver, cwl_entity: dict) -> None:
    """
    Processes the outputs of a CWL component (Workflow, CommandLineTool, or ExpressionTool)
    For each output the following nodes and edges are created:
    - an out-parameter node with the parameter ID as defined in the component and component ID equal to the path of the componet
    - a data node with component ID of the component and data ID equal to output source defined in the component
    - a data edge from the out-parameter node to the component node
    - a data edge from the out-parameter node to the data node

    Parameters:
        driver (Driver): the driver used to connect to Neo4j
        cwl_entity (dict): the dictionary containing the parsed contents of the CWL component
    """
    component_id = cwl_entity['path']
    for output in cwl_entity['outputs']:
        if type(output) == dict:
            # Create out-parameter node with the parameter ID as defined in the component
            # and component ID equal to the path of the componet
            param_node = ensure_parameter_node(driver, output['id'], component_id, 'out')
            param_node_internal_id = param_node[0]
            # Create out-parameter node with the parameter ID as defined in the component
            # and component ID equal to the path of the componet
            create_out_param_relationship(driver, component_id, param_node_internal_id)
            # Create a data node with component ID of the component and data ID equal to output source defined in the component
            # and a data edge from the out-parameter node to the data node
            if 'outputSource' in output:
                # the output source can be a singular ID or a list of IDs
                if type(output['outputSource']) == str:
                    process_source_relationship(driver, output['outputSource'], component_id, param_node_internal_id)
                elif type(output['outputSource']) == list:
                    for source_id in output['outputSource']:
                        process_source_relationship(driver, source_id, component_id, param_node_internal_id)
   
def process_cwl_steps(driver: Driver, cwl_entity: dict) -> None:    
    """
    Processes the steps of a CWL Workflow component (which we will refer to as outer workflow component). 
    A step can be a Workflow, CommandLineTool or ExpressionTool. 
    For each step, a component node is created with component ID equal to the path of the step.
    Then, the lists of inputs and outputs are processed.

    - For each input, the following nodes and edges are created:
        - in-parameter node with ID as defined in the component and component ID equal to the path of the step
        - a data edge from the step component node to the in-parameter node
        - potentially a data node corresponding to the source of the input, with ID equal to the source ID defined in the outer workflow 
        and component ID equal to the path of the outer workflow
        - potentially a data edge from the in-parameter node to the data node of the source

    - If the step has a "when" field, then the JS expression is parsed and its dependencies are extracted.
        - The step is control dependent on data node x with component_id equal to the outer workflow id if:
            - the when expression mentions a step parameter which is data dependent on x
            - the when expression mentions the data_id of x
        - A control edge is created from the step component node to the data node x.

    - For each output, the following nodes and edges are created:
        - out-parameter node with ID as defined in the component and component ID equal to the path of the step
        - a data edge from the out-parameter node to the step component node
        - a data node representing the outer-workflow-level output, with ID equal to [step id]/[output id as defined in workflow]
        and component ID equal to the path of the outer workflow
        - a data edge from the out-parameter node to the data node

    Parameters:
        driver (Driver): the driver used to connect to Neo4j
        cwl_entity (dict): the dictionary containing the parsed contents of the CWL component
    """
    for step in cwl_entity['steps']:

        # Retrieve path of the step
        workflow_folder = Path(cwl_entity['path']).parent
        full_step_path = workflow_folder / Path(step['run'])
        step_path = str(resolve_relative_path(full_step_path))

        # Create the step component node with ID equal to the step 
        s_node = ensure_component_node(driver, step_path)
        s_node_internal_id = s_node[0]

        # Process the list of inputs of the step 
        for input in step['in']:
            # Create in-parameter node with ID as defined in the component and component ID equal to the path of the step
            param_node = ensure_parameter_node(driver, input['id'], step_path, 'in')
            param_node_internal_id = param_node[0]
            # Create a data edge from the step component node to the in-parameter node
            create_data_relationship(driver, s_node_internal_id, param_node_internal_id)

            # Inputs can have one or multiple data sources (data nodes)
            # A data edge is drawn from the in-parameter node to the data node of the source
            if 'source' in input:
                if type(input['source']) == str:
                    source_id = input['source']
                    process_source_relationship(driver, source_id, cwl_entity['path'], param_node_internal_id)
                elif type(input['source']) == list:
                    for source_id in input['source']:
                        process_source_relationship(driver, source_id, cwl_entity['path'], param_node_internal_id)

        # Process the "when" field, aka control dependencies
        if 'when' in step:
            when_expr = step['when']
            expr_tree = parse_javascript_expression_string(when_expr)
            when_refs = traverse_when_statement_extract_dependencies(expr_tree)

            data_nodes = []
            for ref in when_refs:
                ref_id = ref[1]
                if ref[0] == "parameter":
                    input_data = get_wf_data_nodes_from_step_in_param(driver, ref_id, step_path, cwl_entity['path'])
                    data_nodes.extend(input_data)
                elif ref[0] == "step_output":
                    step_output = ensure_data_node(driver, ref_id, cwl_entity['path'])[0]
                    data_nodes.append(step_output)

            for data_node in data_nodes:
                create_control_relationship(driver, s_node_internal_id, data_node)

        # Process the list of outputs of the step
        for output in step['out']:
            # An output can be defined as a dictionary or simply as a string (ID only)
            if type(output) == dict:
                output_id = output['id']
            else:
                output_id = output
            # Create out-parameter node with ID as defined in the component and component ID equal to the path of the step
            param_node = ensure_parameter_node(driver, output_id, step_path, 'out')
            param_node_internal_id = param_node[0]
            # Create a data edge from out-parameter node to the step component node
            create_data_relationship(driver, param_node_internal_id, s_node_internal_id)
            # Create data node with id equal to step_id/output_id and  component ID equal to the path of the outer workflow
            outer_output_id = f"{step['id']}/{output_id}"
            data_node = ensure_data_node(driver, outer_output_id, cwl_entity['path'])
            data_node_internal_id = data_node[0]
            # Create a data edge from the out-parameter node to the data node
            create_data_relationship(driver, param_node_internal_id, data_node_internal_id)