
from neo4j import Driver
from neo4j_queries.utils import clean_component_id

def ensure_component_node(driver: Driver, prefixed_component_id: str) -> tuple[int,str]:
    """
    Ensures that there exists a component node corresponding to the file with local path prefixed_component_id.
    The ID of the component can be given based on the local relative path, so it is cleaned 
    before querying Neo4j.

    Parameters:
        driver (Driver): the Neo4j driver
        prefixed_component_id (str): the local relative path of the component

    Returns:
        tuple[int,str]: the Neoj4 internal ID of the component node, the component ID of the component
    """
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MERGE (c:Component {component_id: $component_id})
    RETURN elementId(c) AS node_internal_id, c.component_id AS component_id
    """
    with driver.session() as session:
        result = session.run(query, component_id=component_id)
        record = result.single()
        return record["node_internal_id"], record["component_id"]

def ensure_parameter_node(driver: Driver, node_id: str, prefixed_component_id: str, param_type: str) \
        -> tuple[int,str,str,str]: 
    """
    Ensures that there exists a parameter node with ID node_id and type param_type
    associated with the component in the file with local path prefixed_component_id.
    The ID of the component can be given based on the local relative path, so it is cleaned 
    before querying Neo4j.

    Parameters:
        driver (Driver): the Neo4j driver
        node_id (str): the ID of the parameter
        prefixed_component_id (str): the local relative path of the component
        param_type (str): the type of the parameter ('in' or 'out')

    Returns:
        tuple[int,str,str, str]: the Neoj4 internal ID of the parameter node, the parameter ID, the component ID, the parameter type
    """
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MERGE (n:Parameter {parameter_id: $node_id, component_id: $component_id, parameter_type: $param_type})
    RETURN elementId(n) AS node_internal_id, n.parameter_id AS id_property, n.component_id AS component_id_property,
        n.parameter_type AS parameter_type_property
    """
    with driver.session() as session:
        result = session.run(query, node_id=node_id, component_id=component_id, param_type=param_type)
        record = result.single()
        return record["node_internal_id"], record["id_property"], record["component_id_property"], record['parameter_type_property']
    
def ensure_data_node(driver: Driver, node_id: str, prefixed_component_id: str) -> tuple[int,str,str]:
    """
    Ensures that there exists a data node with ID node_id
    associated with the component in the file with local path prefixed_component_id.
    The ID of the component can be given based on the local relative path, so it is cleaned 
    before querying Neo4j.

    Parameters:
        driver (Driver): the Neo4j driver
        node_id (str): the ID of the data 
        prefixed_component_id (str): the local relative path of the component

    Returns:
        tuple[int,str,str]: the Neoj4 internal ID of the data node, the data ID, the component ID
    """
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MERGE (n:Data {data_id: $node_id, component_id: $component_id})
    RETURN elementId(n) AS node_internal_id, n.data_id AS id_property, n.component_id AS component_id_property
    """
    with driver.session() as session:
        result = session.run(query, node_id=node_id, component_id=component_id)
        record = result.single()
        return record["node_internal_id"], record["id_property"], record["component_id_property"]
    

def create_ast_node(driver, rule, text):
    query = """
    CREATE (n:ASTNode {rule: $rule, text: $text})
    RETURN elementId(n) AS node_id
    """
    with driver.session() as session:
        result = session.run(query, rule=rule, text=text)
        record = result.single()
        return record["node_id"]
    
def get_wf_data_nodes_from_step_in_param(driver: Driver, param_id: str, prefixed_step_id: str, prefixed_workflow_id: str) -> list[int]:
    """
    Retrieves the internal IDs of data nodes (in a Neo4j database) belonging to the workflow with ID workflow_id 
    such that the in parameter with ID param_id of workflow step step_id has a data dependency on these data nodes.
    This means that in said workflow these data nodes are injected into the parameter param_id of the step.
    The ID of the component can be given based on the local relative path, so it is cleaned 
    before querying Neo4j.

    Parameters:
        param_id: the parameter ID of the step parameter
        prefixed_step_id: the unique ID of the step
        prefixed_workflow_id: the unique ID of the workflow the step is part of
    
    Returns:
        list[int]: the Neo4j internal IDs of the data nodes connected to the parameter node of the step in the mentioned workflow
    """  
    step_id = clean_component_id(prefixed_step_id)
    workflow_id = clean_component_id(prefixed_workflow_id)

    query = """
    MATCH (n1:Data {component_id: $workflow_id})<-[:DATA]-(n2:Parameter {component_id: $step_id, parameter_type: "in", parameter_id: $param_id})
    RETURN elementId(n1) AS internal_id
    """
    with driver.session() as session:
        result = session.run(query, workflow_id=workflow_id, step_id=step_id, param_id=param_id)
        return [record["internal_id"] for record in result]
