from neo4j_queries.utils import clean_component_id

def create_in_param_relationship(driver, prefixed_component_id, parameter_internal_id):
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MATCH (c:Component {component_id: $component_id}), (p)
    WHERE id(p) = $parameter_internal_id
    MERGE (c)-[:DATA]->(p)
    RETURN c.id AS component_id, p.parameter_id AS parameter_id
    """
    with driver.session() as session:
        result = session.run(query, component_id=component_id, 
                             parameter_internal_id=parameter_internal_id)
        record = result.single()
        return record["component_id"], record["parameter_id"]
    
def create_out_param_relationship(driver, prefixed_component_id, parameter_internal_id):
    component_id = clean_component_id(prefixed_component_id)
    query = """
    MATCH (c:Component {component_id: $component_id}), (p)
    WHERE id(p) = $parameter_internal_id
    MERGE (c)<-[:DATA]-(p)
    RETURN c.component_id AS component_id, p.parameter_id AS parameter_id
    """
    with driver.session() as session:
        result = session.run(query, component_id=component_id, 
                             parameter_internal_id=parameter_internal_id)
        record = result.single()
        return record["component_id"], record["parameter_id"]
    
def create_data_relationship(driver, from_internal_node_id, to_internal_node_id):
    query = """
    MATCH (a), (b)
    WHERE id(a) = $from_internal_node_id AND id(b) = $to_internal_node_id
    MERGE (a)-[:DATA]->(b)
    RETURN a.id AS id_1, b.id AS id_2
    """
    with driver.session() as session:
        result = session.run(query, from_internal_node_id=from_internal_node_id,
                             to_internal_node_id=to_internal_node_id)
        record = result.single()
        return record["id_1"], record["id_2"]