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
    MERGE (c)-[:DATA {component_id: $component_id, data_id: p.parameter_id}]->(p)
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
    MERGE (c)<-[:DATA {component_id: $component_id, data_id: p.parameter_id}]-(p)
    RETURN c.component_id AS component_id, p.parameter_id AS parameter_id
    """
    with driver.session() as session:
        result = session.run(query, component_id=component_id, 
                             parameter_internal_id=parameter_internal_id)
    
    
def create_data_relationship(driver: Driver, from_internal_node_id: int, to_internal_node_id: int, component_id: str, data_id: str)  -> tuple[int,int]:
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
    clean_id = clean_component_id(component_id)
    query = """
    MATCH (a), (b)
    WHERE elementId(a) = $from_internal_node_id AND elementId(b) = $to_internal_node_id
    MERGE (a)-[:DATA {component_id: $component_id, data_id: $data_id}]->(b)
    RETURN elementId(a) AS id_1, elementId(b) AS id_2
    """
    with driver.session() as session:
        result = session.run(query, from_internal_node_id=from_internal_node_id,
                             to_internal_node_id=to_internal_node_id, component_id= clean_id, data_id=data_id)
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
    

    
def create_references_relationship(driver: Driver, prefixed_component_id: int, git_internal_node_id: int, reference: str)  -> tuple[int,int]:
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MATCH (component: Component), (git)
    WHERE component.component_id = $component_id AND elementId(git) = $git_internal_node_id
    MERGE (component)-[:REFERENCES{component_id: $component_id, reference: $reference}]->(git)
    RETURN elementId(component) AS id_1, elementId(git) AS id_2
    """
    with driver.session() as session:
        result = session.run(query, component_id=component_id,
                             git_internal_node_id=git_internal_node_id, reference=reference)
        record = result.single()
        return record["id_1"], record["id_2"]
    
def clean_relationship(driver: Driver)  -> tuple[int,int]:
    query = """
    MATCH ()-[r:REFERENCES]-()
    WHERE r.reference IS NULL
    DELETE r
    """
    with driver.session() as session:
        session.run(query)