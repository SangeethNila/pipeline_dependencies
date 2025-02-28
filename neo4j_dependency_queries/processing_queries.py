from neo4j import Session


def get_component_id_labels_of_node(session: Session, node_id):
    query = """
    MATCH (n) WHERE elementId(n) = $node_id
    RETURN n.component_id AS component_id, labels(n) AS nodeLabels, n.entity_type AS entityType,
    CASE WHEN n.workflow_list IS NOT NULL THEN n.workflow_list ELSE null END AS workflowList
    """
    result = session.run(query, node_id=node_id)
    record = result.single()
    component_id = record["component_id"]
    node_labels = record["nodeLabels"]
    entity_type = record["entityType"]
    workflow_list = record["workflowList"]

    return component_id, node_labels, entity_type, workflow_list

def get_valid_connections(session: Session, node_id, allowed_component_ids):
    query = """
    MATCH (n)-[r]->(m)
    WHERE elementId(n) = $node_id AND r.component_id IN $allowed_component_ids
    RETURN elementId(m) AS nextNodeId, elementId(r) AS relId, m.component_id AS nextComponentId, 
        labels(m) AS nodeLabels, type(r) AS relType, r.component_id AS relComponentId,
        r.data_ids AS dataIds,
        CASE WHEN n.entity_type IS NOT NULL THEN n.entity_type ELSE null END AS entityType,
        CASE WHEN m.entity_type IS NOT NULL THEN m.entity_type ELSE null END AS nextEntityType,
        CASE WHEN r.workflow_list IS NOT NULL THEN r.workflow_list ELSE null END AS workflowList
    """
    results = session.run(query, node_id=node_id, allowed_component_ids=list(allowed_component_ids))
    return results


def update_workflow_list_of_edge(session: Session, edge_id: str, workflow_ids: list):
    query = """MATCH ()-[r]->()
        WHERE elementId(r) = $edge_id
        SET r.workflow_list = 
            CASE 
                WHEN r.workflow_list IS NULL THEN $workflow_ids
                ELSE r.workflow_list + [x IN $workflow_ids WHERE NOT x IN r.workflow_list]
            END
        RETURN r.workflow_list"""
    session.run(query, edge_id=edge_id, workflow_ids=workflow_ids)

def update_workflow_list_of_node(session: Session, node_id: str, workflow_ids: list):
    query = """MATCH (m)-[]->()
        WHERE elementId(m) = $node_id
        SET m.workflow_list = 
            CASE 
                WHEN m.workflow_list IS NULL THEN $workflow_ids
                ELSE m.workflow_list + [x IN $workflow_ids WHERE NOT x IN m.workflow_list]
            END
        RETURN m.workflow_list"""
    session.run(query, node_id=node_id, workflow_ids=workflow_ids)


def get_all_workflow_ids(session: Session):
    query = """MATCH (n:InParameter)
        WHERE n.entity_type = "Workflow"
        RETURN COLLECT(DISTINCT n.component_id) AS unique_component_ids;
        """
    result = session.run(query)
    unique_component_ids = result.single()["unique_component_ids"]
    return unique_component_ids

def get_all_outermost_workflow_ids(session: Session):
    query = """MATCH (n:InParameter)
        WHERE n.entity_type = "Workflow" AND NOT (n)-[]->()
        RETURN COLLECT(DISTINCT n.component_id) AS unique_component_ids;
        """
    result = session.run(query)
    unique_component_ids = result.single()["unique_component_ids"]
    print(unique_component_ids)
    return unique_component_ids

def get_all_out_parameter_nodes_of_entity(session: Session, component_id: str):
    query = """
            MATCH (n:OutParameter {component_id: $component_id})
            RETURN elementId(n) AS nodeId, n.entity_type AS entityType
            """
    result = session.run(query, component_id=component_id)
    return result