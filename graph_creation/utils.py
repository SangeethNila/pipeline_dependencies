from neo4j import Driver
from neo4j_queries.node_queries import ensure_data_node, ensure_parameter_node
from neo4j_queries.edge_queries import create_data_relationship, create_in_param_relationship

def create_input_nodes_and_relationships(driver: Driver, input_id: str, component_id: str) -> None:
    # Create in-parameter node i_node with id = i.id and component_id = c_node.id
    param_node = ensure_parameter_node(driver, input_id, component_id, 'in')
    param_node_internal_id = param_node[0]
    # Create a directed data edge from c_node to i_node
    create_in_param_relationship(driver, component_id, param_node_internal_id)
    # Create a data node i_data_node with id = i.id and component_id = c_node.id
    data_node = ensure_data_node(driver, input_id, component_id)
    data_node_internal_id = data_node[0]
    # Create a data edge from i_data_node to i_node
    create_data_relationship(driver, data_node_internal_id, param_node_internal_id)

def process_source_relationship(driver: Driver, source_id: str, component_id: str, param_node_internal_id: str) -> None:
    data_node = ensure_data_node(driver, source_id, component_id)
    data_node_internal_id = data_node[0]
    create_data_relationship(driver, param_node_internal_id, data_node_internal_id)