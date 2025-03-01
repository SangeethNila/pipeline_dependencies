import json
from neo4j import Driver
from neo4j_dependency_queries.utils import clean_component_id

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
    
def ensure_git_node(driver: Driver, git_url: str) -> tuple[int,str]:

    query = """
    MERGE (c:Git {git_url: $git_url})
    RETURN elementId(c) AS node_internal_id, c.git_url AS git_url
    """
    with driver.session() as session:
        result = session.run(query, git_url=git_url)
        record = result.single()
        return record["node_internal_id"], record["git_url"]

def ensure_in_parameter_node(driver: Driver, parameter_id: str, prefixed_component_id: str, param_type: str = None, entity_type: str = None) \
        -> tuple[int,str,str,str]: 
    """
    Ensures that there exists an  in-parameter node with ID node_id
    associated with the component in the file with local path prefixed_component_id.
    The ID of the component can be given based on the local relative path, so it is cleaned 
    before querying Neo4j.

    Parameters:
        driver (Driver): the Neo4j driver
        parameter_id (str): the ID of the parameter
        prefixed_component_id (str): the local relative path of the component
        type (str): the parameter type

    Returns:
        tuple[int,str,str]: the Neoj4 internal ID of the parameter node, the parameter ID, the component ID
    """
    component_id = clean_component_id(prefixed_component_id)
    query_type = """
    MERGE (n:InParameter {parameter_id: $parameter_id, component_id: $component_id})
    SET n.type = $type
    SET n.entity_type = $entity_type
    RETURN elementId(n) AS node_id, n.parameter_id AS parameter_id, n.component_id AS component_id
    """
    query_check = """
    MERGE (n:InParameter {parameter_id: $parameter_id, component_id: $component_id})
    RETURN elementId(n) AS node_id, n.parameter_id AS parameter_id, n.component_id AS component_id
    """
    with driver.session() as session:
        if param_type and entity_type:
            query = query_type
        else:
            query = query_check
        new_param_type = param_type
        if isinstance(param_type, dict):
            new_param_type = str(param_type)
        elif isinstance(param_type, list):
            new_list = [str(type) for type in param_type]
            new_param_type = " OR ".join(new_list)
        result = session.run(query, parameter_id=parameter_id, component_id=component_id, type=new_param_type, entity_type=entity_type)
        record = result.single()
        return record["node_id"], record["parameter_id"], record["component_id"]
    
def ensure_out_parameter_node(driver: Driver, parameter_id: str, prefixed_component_id: str, param_type: str = None, entity_type: str = None) \
        -> tuple[int,str,str,str]: 
    """
    Ensures that there exists an out-parameter node with ID node_id
    associated with the component in the file with local path prefixed_component_id.
    The ID of the component can be given based on the local relative path, so it is cleaned 
    before querying Neo4j.

    Parameters:
        driver (Driver): the Neo4j driver
        parameter_id (str): the ID of the parameter
        prefixed_component_id (str): the local relative path of the component
        type (str): the parameter type

    Returns:
        tuple[int,str,str]: the Neoj4 internal ID of the parameter node, the parameter ID, the component ID
    """
    component_id = clean_component_id(prefixed_component_id)
    query_type = """
    MERGE (n:OutParameter {parameter_id: $parameter_id, component_id: $component_id})
    SET n.type = $type
    SET n.entity_type = $entity_type
    RETURN elementId(n) AS node_id, n.parameter_id AS parameter_id, n.component_id AS component_id
    """
    query_check = """
    MERGE (n:OutParameter {parameter_id: $parameter_id, component_id: $component_id})
    RETURN elementId(n) AS node_id, n.parameter_id AS parameter_id, n.component_id AS component_id
    """


    if param_type and entity_type:
        query = query_type
    else:
        query = query_check

    with driver.session() as session:
        new_param_type = param_type
        if isinstance(param_type, dict):
            new_param_type = str(param_type)
        elif isinstance(param_type, list):
            new_list = [str(type) for type in param_type]
            new_param_type = " OR ".join(new_list)
        result = session.run(query, parameter_id=parameter_id, component_id=component_id, type=new_param_type, entity_type=entity_type)
        record = result.single()
        return record["node_id"], record["parameter_id"], record["component_id"]
    
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
    