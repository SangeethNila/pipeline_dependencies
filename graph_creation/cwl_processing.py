from neo4j import Driver
from graph_creation.utils import create_input_nodes_and_relationships, process_source_relationship
from neo4j_queries.node_queries import ensure_component_node, ensure_data_node, ensure_parameter_node
from neo4j_queries.edge_queries import create_data_relationship, create_out_param_relationship
from pathlib import Path

def process_cwl_inputs(driver: Driver, cwl_entity: dict) -> None:
    component_id = cwl_entity['path']
    if type(cwl_entity['inputs']) == list:
        for input in cwl_entity['inputs']:
            if type(input) == dict:
                create_input_nodes_and_relationships(driver, input['id'], component_id)
    elif type(cwl_entity['inputs']) == dict:
        for key in cwl_entity['inputs'].keys():
            create_input_nodes_and_relationships(driver, key, component_id)

def process_cwl_outputs(driver: Driver, cwl_entity: dict) -> None:
    component_id = cwl_entity['path']
    for output in cwl_entity['outputs']:
        if type(output) == dict:
            # Create out-parameter node o_node with id = o.id and component_id = c_node.id
            param_node = ensure_parameter_node(driver, output['id'], component_id, 'out')
            # Create a directed data edge from o_node to c_node
            param_node_internal_id = param_node[0]
            create_out_param_relationship(driver, component_id, param_node_internal_id)
            if 'outputSource' in output:
                if type(output['outputSource']) == str:
                    process_source_relationship(driver, output['outputSource'], component_id, param_node_internal_id)
                elif type(output['outputSource']) == list:
                    for o in output['outputSource']:
                        process_source_relationship(driver, o, component_id, param_node_internal_id)
                        
def process_cwl_steps(driver: Driver, cwl_entity: dict, repo: str) -> None:
    for step in cwl_entity['steps']:
        combined_path = Path(repo) / step['run']
        step_path = str(combined_path)
        # if a component node with the same path (run) as s does not exist then
        # Create component node s_node unique to s with id equal to run 
        s_node = ensure_component_node(driver, step_path)
        s_node_internal_id = s_node[0]
        for i in step['in']:
            # Create in-parameter node i_node with id = i.id and component_id = s.run
            param_node = ensure_parameter_node(driver, i['id'], step_path, 'in')
            param_node_internal_id = param_node[0]
            # Create a data edge from s_node to i_node
            create_data_relationship(driver, s_node_internal_id, param_node_internal_id)

            if 'source' in i:
                if type(i['source']) == str:
                    source_id = i['source']
                    process_source_relationship(driver, source_id, cwl_entity['path'], param_node_internal_id)
                elif type(i['source']) == list:
                    for source_id in i['source']:
                        process_source_relationship(driver, source_id, cwl_entity['path'], param_node_internal_id)

        for o in step['out']:
            if type(o) == dict:
                o_id = o['id']
            else:
                o_id = o
            # Create out-parameter node o_node with id = o.id and component_id = s.run
            param_node = ensure_parameter_node(driver, o_id, step_path, 'out')
            param_node_internal_id = param_node[0]
            # Create a data edge from o_node to s_node
            create_data_relationship(driver, param_node_internal_id, s_node_internal_id)
            # Workflow-level outputs of a step have \texttt{id} corresponding to \texttt{[[step ID]/[output ID as defined in workflow]]} 
            # and a \texttt{component\_id} property equal to the ID of the workflow
            # Create data node o_data_node with id = step_id/output_id and component_id = c_node.id
            output_id = f"{step['id']}/{o_id}"
            data_node = ensure_data_node(driver, output_id, cwl_entity['path'])
            data_node_internal_id = data_node[0]
            # Create a data edge from o_node to o_data_node
            create_data_relationship(driver, param_node_internal_id, data_node_internal_id)