
from neo4j import Driver
from neo4j_queries.utils import clean_component_id

def ensure_component_node(driver: Driver, prefixed_component_id: str) -> tuple[int,str]:
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MERGE (c:Component {component_id: $component_id})
    RETURN id(c) AS node_internal_id, c.id AS id_property
    """
    with driver.session() as session:
        result = session.run(query, component_id=component_id)
        record = result.single()
        return record["node_internal_id"], record["id_property"]

def ensure_parameter_node(driver: Driver, node_id: str, prefixed_component_id: str, param_type: str) \
        -> tuple[int,str,str,str]: 
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MERGE (n:Parameter {parameter_id: $node_id, component_id: $component_id})
    ON CREATE SET 
        n.component_id = $component_id,
        n.parameter_type = $param_type
    RETURN id(n) AS node_internal_id, n.parameter_id AS id_property, n.component_id AS component_id_property,
        n.parameter_type AS parameter_type_property
    """
    with driver.session() as session:
        result = session.run(query, node_id=node_id, component_id=component_id, param_type=param_type)
        record = result.single()
        return record["node_internal_id"], record["id_property"], record["component_id_property"], record['parameter_type_property']
    
def ensure_data_node(driver: Driver, node_id: str, prefixed_component_id: str) -> tuple[int,str,str]:
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MERGE (n:Data {data_id: $node_id, component_id: $component_id})
    ON CREATE SET 
        n.component_id = $component_id
    RETURN id(n) AS node_internal_id, n.data_id AS id_property, n.component_id AS component_id_property
    """
    with driver.session() as session:
        result = session.run(query, node_id=node_id, component_id=component_id)
        record = result.single()
        return record["node_internal_id"], record["id_property"], record["component_id_property"]