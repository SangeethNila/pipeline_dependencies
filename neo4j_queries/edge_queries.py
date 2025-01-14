from neo4j import Driver
from neo4j_queries.utils import clean_component_id

def create_in_param_relationship(driver: Driver, prefixed_component_id: str, parameter_internal_id: int) -> tuple[str,str]:
    """
    Creates a data dependency relationship in Neo4j between a component node with path prefixed_component_id 
    and an in-parameter node with Neo4j internal ID parameter_internal_id.
    This relationship is an outgoing data edge from the component to the in-parameter node.
    The ID of the component can be given based on the local relative path, so it needs to be cleaned 
    before querying Neo4j.

    Parameters:
        driver (Driver): the Neo4j driver
        prefixed_component_id (str): the local relative path of the component
        parameter_internal_id (int): the internal Neo4j ID of the in-parameter node

    Returns:
        tuple[str,str]: the component ID of the component, the parameter ID of the parameter
    """
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MATCH (c:Component {component_id: $component_id}), (p:InParameter)
    WHERE elementId(p) = $parameter_internal_id
    MERGE (c)-[:DATA]->(p)
    RETURN c.component_id AS component_id, p.parameter_id AS parameter_id
    """
    with driver.session() as session:
        result = session.run(query, component_id=component_id, 
                             parameter_internal_id=parameter_internal_id)
    
def create_out_param_relationship(driver: Driver, prefixed_component_id: str, parameter_internal_id: int) -> tuple[str,str]:
    """
    Creates a data dependency relationship in Neo4j between a component node with path prefixed_component_id 
    and an out-parameter node with Neo4j internal ID parameter_internal_id.
    This relationship is an outgoing data edge from the out-parameter to the component node.
    The ID of the component can be given based on the local relative path, so it needs to be cleaned 
    before querying Neo4j.

    Parameters:
        driver (Driver): the Neo4j driver
        prefixed_component_id (str): the local relative path of the component
        parameter_internal_id (int): the internal Neo4j ID of the out-parameter node

    Returns:
        tuple[str,str]: the component ID of the component, the parameter ID of the parameter
    """
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MATCH (c:Component {component_id: $component_id}), (p: OutParameter)
    WHERE elementId(p) = $parameter_internal_id
    MERGE (c)<-[:DATA]-(p)
    RETURN c.component_id AS component_id, p.parameter_id AS parameter_id
    """
    with driver.session() as session:
        result = session.run(query, component_id=component_id, 
                             parameter_internal_id=parameter_internal_id)
    
def create_data_relationship(driver: Driver, from_internal_node_id: int, to_internal_node_id: int)  -> tuple[int,int]:
    """
    Creates a data dependency relationship in Neo4j between the two nodes with Neo4j internal IDs given as parameters.
    This relationship is an outgoing data edge from the node with internal ID from_internal_node_id
    to the node with internal ID to_internal_node_id.

    Parameters:
        driver (Driver): the Neo4j driver
        from_internal_node_id (int): the internal Neo4j ID of the first node
        to_internal_node_id (int): the internal Neo4j ID of the second node

    Returns:
        tuple[int,int]: from_internal_node_id, to_internal_node_id
    """
    query = """
    MATCH (a), (b)
    WHERE elementId(a) = $from_internal_node_id AND elementId(b) = $to_internal_node_id
    MERGE (a)-[:DATA]->(b)
    RETURN elementId(a) AS id_1, elementId(b) AS id_2
    """
    with driver.session() as session:
        result = session.run(query, from_internal_node_id=from_internal_node_id,
                             to_internal_node_id=to_internal_node_id)
        record = result.single()
        return record["id_1"], record["id_2"]
    
def create_data_relationship_with_id(driver: Driver, from_internal_node_id: int, to_internal_node_id: int, id: str)  -> tuple[int,int]:
    """
    Creates a data dependency relationship in Neo4j between the two nodes with Neo4j internal IDs given as parameters.
    This relationship is an outgoing data edge from the node with internal ID from_internal_node_id
    to the node with internal ID to_internal_node_id.

    Parameters:
        driver (Driver): the Neo4j driver
        from_internal_node_id (int): the internal Neo4j ID of the first node
        to_internal_node_id (int): the internal Neo4j ID of the second node

    Returns:
        tuple[int,int]: from_internal_node_id, to_internal_node_id
    """
    query = """
    MATCH (a), (b)
    WHERE elementId(a) = $from_internal_node_id AND elementId(b) = $to_internal_node_id
    MERGE (a)-[:DATA {component_id: $component_id}]->(b)
    RETURN elementId(a) AS id_1, elementId(b) AS id_2
    """
    with driver.session() as session:
        result = session.run(query, from_internal_node_id=from_internal_node_id,
                             to_internal_node_id=to_internal_node_id, component_id= id)
        record = result.single()
        return record["id_1"], record["id_2"]
    

def create_control_relationship(driver: Driver, from_internal_node_id: int, to_internal_node_id: int, component_id: str)  -> tuple[int,int]:
    """
    Creates a control dependency relationship in Neo4j between the two nodes with Neo4j internal IDs given as parameters.
    This relationship is an outgoing control edge from the node with internal ID from_internal_node_id
    to the node with internal ID to_internal_node_id.

    Parameters:
        driver (Driver): the Neo4j driver
        from_internal_node_id (int): the internal Neo4j ID of the first node
        to_internal_node_id (int): the internal Neo4j ID of the second node

    Returns:
        tuple[int,int]: from_internal_node_id, to_internal_node_id
    """
    query = """
    MATCH (a), (b)
    WHERE elementId(a) = $from_internal_node_id AND elementId(b) = $to_internal_node_id
    MERGE (a)-[:CONTROL {component_id: $component_id}]->(b)
    RETURN elementId(a) AS id_1, elementId(b) AS id_2
    """
    with driver.session() as session:
        result = session.run(query, from_internal_node_id=from_internal_node_id,
                             to_internal_node_id=to_internal_node_id, component_id=component_id)
        record = result.single()
        return record["id_1"], record["id_2"]
    
def create_has_child_relationship(driver: Driver, parent_internal_node_id: int, child_internal_node_id: int)  -> tuple[int,int]:
    """
    Creates a "has child" relationship in Neo4j between the two nodes with Neo4j internal IDs given as parameters.
    This relationship is an outgoing "has child" edge from the parent node to the child node.

    Parameters:
        driver (Driver): the Neo4j driver
        parent_internal_node_id (int): the internal Neo4j ID of the parent node
        child_internal_node_id (int): the internal Neo4j ID of the child node

    Returns:
        tuple[int,int]: parent_internal_node_id, child_internal_node_id
    """
    query = """
    MATCH (parent), (child)
    WHERE elementId(parent) = $parent_id AND elementId(child) = $child_id
    MERGE (parent)-[:HAS_CHILD]->(child)
    RETURN elementId(parent) AS id_1, elementId(child) AS id_2
    """
    with driver.session() as session:
        result = session.run(query, parent_id=parent_internal_node_id,
                             child_id=child_internal_node_id)
        record = result.single()
        return record["id_1"], record["id_2"]
    
def simplify_data_and_control_edges(driver: Driver):
    with driver.session() as session:
        create_data_edges_query = """
        MATCH (n1)-[:DATA]->(n:Data), (n)-[:DATA]->(n2)
        WITH n, n1, n2, n.component_id AS component_id, n.data_id AS data_id
        MERGE (n1)-[:DATA {component_id: component_id, data_id: data_id}]->(n2)
        """
        session.run(create_data_edges_query)

        create_control_edges_query = """
        MATCH (n1)-[:CONTROL]->(n:Data), (n)-[:DATA]->(n2)
        WITH n, n1, n2, n.component_id AS component_id, n.data_id AS data_id
        MERGE (n1)-[:CONTROL {component_id: component_id, data_id: data_id}]->(n2)
        """
        session.run(create_control_edges_query)
        
        delete_data_query = """
        MATCH (n:Data)
        DETACH DELETE n
        """
        session.run(delete_data_query)