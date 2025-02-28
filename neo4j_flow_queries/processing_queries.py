from neo4j import Session


def get_outer_workflow_ids(session: Session, component_id: str):
    query = """
    MATCH (c:CalculationComponent {component_id: $component_id})-[e]-()
    RETURN collect(distinct e.workflow_id) AS workflows
    """
    workflows = set(session.run(query, component_id=component_id).single()["workflows"])
    return workflows

def get_all_component_ids(session: Session):
    """Fetches all component_ids of CalculationComponent nodes."""
    query = """
    MATCH (c:CalculationComponent)
    RETURN c.component_id AS component_id
    """
    result = session.run(query)
    return [record["component_id"] for record in result]

def get_all_workflow_ids(session: Session):
    """Fetches all component_ids of CalculationComponent nodes."""
    query = """
    MATCH (c:CalculationComponent)
    WHERE c.entity_type = "Workflow"
    RETURN c.component_id AS component_id
    """
    result = session.run(query)
    return [record["component_id"] for record in result]

def get_indirect_flow_connections(session: Session, component_id: str, workflow_id: str):
    query = """
        MATCH (c1:CalculationComponent2 {component_id: $component_id})-[r:INDIRECT_FLOW {workflow_id: $workflow_id}]->(c2:CalculationComponent2)
        RETURN c2.component_id AS next_component_id, elementId(r) AS edge_id
        """
    result = session.run(query, component_id=component_id, workflow_id=workflow_id)
    return result

def get_sequential_indirect_flow_connections(session: Session, component_id: str, workflow_id: str):
    query = """
        MATCH (c1:CalculationComponent2 {component_id: $component_id})-[r:SEQUENTIAL_INDIRECT_FLOW {workflow_id: $workflow_id}]->(c2:CalculationComponent2)
        RETURN c2.component_id AS next_component_id, elementId(r) AS edge_id
        """
    result = session.run(query, component_id=component_id, workflow_id=workflow_id)
    return result

def fetch_calculation_component_fan_data(session: Session):
    """Queries Neo4j for CalculationComponent fan-in, fan-out, and fan-out set."""
    query = """
    MATCH (cc:CalculationComponent)
    OPTIONAL MATCH (cc)<-[r_in]-(target_in)  // Count all incoming relationships
    OPTIONAL MATCH (cc)-[r_out]->(target_out)  // Count all outgoing relationships & get targets
    RETURN cc.component_id AS component_id, 
            COUNT(DISTINCT r_in) AS fan_in, 
            COUNT(DISTINCT r_out) AS fan_out,
            COLLECT(DISTINCT target_in.component_id) AS fan_in_set,
            COLLECT(DISTINCT target_out.component_id) AS fan_out_set
    """
    
    result = session.run(query)
    return [{"component_id": record["component_id"], 
                "fan_in": record["fan_in"], 
                "fan_out": record["fan_out"], 
                "fan_in_set": record["fan_in_set"],
                "fan_out_set": record["fan_out_set"]} 
            for record in result]