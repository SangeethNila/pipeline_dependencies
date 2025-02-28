from neo4j import Session


def get_component_id_labels_of_node(session: Session, node_id):
    query = """
    MATCH (n) WHERE elementId(n) = $node_id
    RETURN n.component_id AS component_id, labels(n) AS nodeLabels
    """
    result = session.run(query, node_id=node_id)
    record = result.single()
    component_id = record["component_id"]
    node_labels = record["nodeLabels"]
    return component_id, node_labels

def get_valid_connections(session: Session, node_id, allowed_component_ids):
    query = """
    MATCH (n)-[r]-(m)
    WHERE elementId(n) = $node_id AND r.component_id IN $allowed_component_ids
    RETURN elementId(m) AS nextNodeId, elementId(r) AS relId, m.component_id AS nextComponentId, 
        labels(m) AS nodeLabels, type(r) AS relType, r.component_id AS relComponentId,
        r.data_ids AS dataIds,
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
    return unique_component_ids
