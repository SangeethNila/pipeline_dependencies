from neo4j import Session

from neo4j_graph_queries.utils import clean_component_id


def get_node_details(session: Session, node_id: str):

    query = f"""
    MATCH (n) WHERE elementId(n) = $node_id
    RETURN n.component_id AS component_id, labels(n) AS nodeLabels, n.component_type AS componentType
    """
    result = session.run(query, node_id=node_id)
    record = result.single()
    component_id = record["component_id"]
    node_labels = record["nodeLabels"]
    component_type = record["componentType"]

    return component_id, node_labels, component_type

def get_nodes_with_control_edges(session: Session):
    query = """
    MATCH (m)-[r]->(n)
    WHERE type(r) = 'CONTROL_DEPENDENCY'
    RETURN elementId(m) AS sourceId, elementId(n) AS targetId, r.component_id AS componentId, elementId(r) AS edgeId
    """
    result = session.run(query)

    return result

def get_valid_connections(session: Session, node_id: str, component_id: str, step_id: str = None):
    """
    Retrieves valid connections from a given node in the Neo4j graph database.

    Queries for outgoing `DATA_FLOW` edges with specified `component_id` from a node with `node_id`. 
    If `step_id` is provided, it further filters the results to include only relationships where 
    `r.data_id` starts with `{step_id}/`.

    Parameters:
        session (Session): The Neo4j database session.
        node_id (str): The element ID of the starting node.
        component_id (str): The component ID to filter the relationships.
        step_id (str, optional): A prefix filter for `r.data_id` (must start with `{step_id}/`). Defaults to None.

    Returns:
        neo4j.Result: The query results containing the connected nodes and relationship details.
    """

    query = """
    MATCH (n)-[r:DATA_FLOW]->(m)
    WHERE elementId(n) = $node_id 
        AND r.component_id = $component_id 
    """
    
    if step_id is not None:
        query += "AND r.data_id STARTS WITH $step_id + '/' \n"

    query += """
    RETURN elementId(m) AS nextNodeId, elementId(r) AS relId, m.component_id AS nextComponentId, 
        labels(m) AS nodeLabels, type(r) AS relType, r.component_id AS relComponentId,
        r.data_id AS dataId, r.step_id AS stepId,
        CASE WHEN n.component_type IS NOT NULL THEN n.component_type ELSE null END AS componentType,
        CASE WHEN m.component_type IS NOT NULL THEN m.component_type ELSE null END AS nextEntityType,
        CASE WHEN r.workflow_list IS NOT NULL THEN r.workflow_list ELSE null END AS workflowList
    """
    
    params = {"node_id": node_id, "component_id": component_id}
    if step_id is not None:
        params["step_id"] = step_id

    results = session.run(query, **params)
    return results

def get_all_outgoing_edges(session: Session, node_id):
    query = """
    MATCH (n)-[r]->(m)
    WHERE elementId(n) = $node_id 
    RETURN elementId(m) AS nextNodeId, elementId(r) AS relId, m.component_id AS nextComponentId, 
        labels(m) AS nextNodeLabels, type(r) AS relType, r.component_id AS relComponentId,
        r.data_ids AS dataIds,
        CASE WHEN n.component_type IS NOT NULL THEN n.component_type ELSE null END AS componentType,
        CASE WHEN m.component_type IS NOT NULL THEN m.component_type ELSE null END AS nextEntityType,
        CASE WHEN r.workflow_list IS NOT NULL THEN r.workflow_list ELSE null END AS workflowList
    """
    results = session.run(query, node_id=node_id)
    return results


def update_workflow_list_of_edge(session: Session, edge_id: str, workflow_ids: list):
    query = """MATCH ()-[r]->()
        WHERE elementId(r) = $edge_id
        WITH r, apoc.coll.toSet(r.workflow_list + $workflow_ids) AS combined_list
        SET r.workflow_list = apoc.coll.sort(combined_list)
        RETURN r.workflow_list
        """
    session.run(query, edge_id=edge_id, workflow_ids=workflow_ids)

def get_workflow_list_of_data_edges_from_node(session: Session, node_id:str, edge_component_id: str):
    query = """MATCH (m)-[r:DATA_FLOW]->(n)
        WHERE elementId(m) = $node_id AND r.component_id = $component_id
        RETURN r.workflow_list AS workflow_list"""
    result = session.run(query, node_id=node_id, component_id=edge_component_id)
    return result

def initiate_workflow_list(session: Session):
    query = """MATCH ()-[r:DATA_FLOW]->()
        WHERE r.workflow_list IS NULL
        SET r.workflow_list = []"""
    session.run(query)


def get_all_workflow_ids(session: Session):
    query = """MATCH (n:InParameter)
        WHERE n.component_type = "Workflow"
        RETURN COLLECT(DISTINCT n.component_id) AS unique_component_ids;
        """
    result = session.run(query)
    unique_component_ids = result.single()["unique_component_ids"]
    return unique_component_ids

def get_all_out_parameter_nodes_of_entity(session: Session, component_id: str):
    query = """
            MATCH (n:OutParameter {component_id: $component_id})
            RETURN elementId(n) AS nodeId, n.component_type AS componentType
            """
    result = session.run(query, component_id=component_id)
    return result

def get_all_in_parameter_nodes_of_entity(session: Session, component_id: str):
    clean_id = clean_component_id(component_id)
    query = """
            MATCH (n:InParameter {component_id: $component_id})
            RETURN elementId(n) AS nodeId, n.component_type AS componentType
            """
    result = session.run(query, component_id=clean_id)
    return result

def get_all_component_ids(session: Session):
    query = """
            MATCH (n:InParameter)
            RETURN collect(distinct n.component_id) AS component_ids
            """
    result = session.run(query)
    return result.single()["component_ids"]

def get_all_outer_out_parameter_nodes(session: Session):
    query = """
            MATCH (n:OutParameter)
            WHERE NOT ()-[]->(n)
            RETURN elementId(n) AS nodeId
            """
    result = session.run(query)
    return result

def get_data_flow_relationships_for_sorting(session: Session):
    query = """
    MATCH (a:InParameter)-[:DATA_FLOW]->(b:InParameter)
    RETURN a.component_id AS componentA, b.component_id AS componentB
    """
    result = session.run(query)
    edges = [(record['componentA'], record['componentB']) for record in result]
    return edges