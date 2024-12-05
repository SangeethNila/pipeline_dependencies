
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

def ensure_in_parameter_node(driver: Driver, node_id: str, prefixed_component_id: str) \
        -> tuple[int,str,str,str]: 
    """
    Ensures that there exists an  in-parameter node with ID node_id
    associated with the component in the file with local path prefixed_component_id.
    The ID of the component can be given based on the local relative path, so it is cleaned 
    before querying Neo4j.

    Parameters:
        driver (Driver): the Neo4j driver
        node_id (str): the ID of the parameter
        prefixed_component_id (str): the local relative path of the component

    Returns:
        tuple[int,str,str]: the Neoj4 internal ID of the parameter node, the parameter ID, the component ID
    """
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MERGE (n:InParameter {parameter_id: $node_id, component_id: $component_id})
    RETURN elementId(n) AS node_internal_id, n.parameter_id AS id_property, n.component_id AS component_id_property
    """
    with driver.session() as session:
        result = session.run(query, node_id=node_id, component_id=component_id)
        record = result.single()
        return record["node_internal_id"], record["id_property"], record["component_id_property"]
    
def ensure_out_parameter_node(driver: Driver, node_id: str, prefixed_component_id: str) \
        -> tuple[int,str,str,str]: 
    """
    Ensures that there exists an out-parameter node with ID node_id
    associated with the component in the file with local path prefixed_component_id.
    The ID of the component can be given based on the local relative path, so it is cleaned 
    before querying Neo4j.

    Parameters:
        driver (Driver): the Neo4j driver
        node_id (str): the ID of the parameter
        prefixed_component_id (str): the local relative path of the component

    Returns:
        tuple[int,str,str]: the Neoj4 internal ID of the parameter node, the parameter ID, the component ID
    """
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MERGE (n:OutParameter {parameter_id: $node_id, component_id: $component_id})
    RETURN elementId(n) AS node_internal_id, n.parameter_id AS id_property, n.component_id AS component_id_property
    """
    with driver.session() as session:
        result = session.run(query, node_id=node_id, component_id=component_id)
        record = result.single()
        return record["node_internal_id"], record["id_property"], record["component_id_property"]
    
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
    

def create_ast_node(driver, component_id, rule, text):
    query = """
    MERGE (n:ASTNode {component_id:$component_id, rule: $rule, text: $text})
    RETURN elementId(n) AS node_id
    """
    with driver.session() as session:
        result = session.run(query, component_id=component_id, rule=rule, text=text)
        record = result.single()
        return record["node_id"]
    