import pathlib

from neo4j import Session

def create_calculation_component_node(session: Session, component_id: str, entity_type: str):
    """Creates a CalculationComponent node for a given component_id if it does not exist."""
    nice_id = pathlib.Path(component_id).stem
    query = """
    MERGE (cc:CalculationComponent {component_id: $component_id})
    SET cc.nice_id = $nice_id
    SET cc.entity_type = $entity_type
    RETURN cc
    """
    session.run(query, component_id=component_id, nice_id=nice_id, entity_type=entity_type)

def create_direct_flow(session: Session, from_component_id: str, to_component_id: str, 
                             workflow_id: str, data_id: str):
    """
    Creates a DIRECT_FLOW relationship between two CalculationComponent nodes.
    A -(DIRECT_FLOW)-> B if A calls B
    This happens when a workflow A calls component B as a step
    """
    description = "Workflow (source) calls step (target)"
    query = """
    MATCH (cc_from:CalculationComponent {component_id: $from_component_id})
    MATCH (cc_to:CalculationComponent {component_id: $to_component_id})
    MERGE (cc_from)-[:DIRECT_FLOW {workflow_id: $workflow_id, description: $description, data_id: $data_id}]->(cc_to)
    """
    session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                description=description, data_id=data_id)
        
def create_indirect_flow(session: Session, from_component_id: str, to_component_id: str, 
                               workflow_id: str, data_id: str):
    """
    Creates an INDIRECT_FLOW relationship between two CalculationComponent nodes.
    A -(INDIRECT_FLOW)-> B
    - Case: if B calls A and A returns a value to B, which B subsequenty uses
            This happens when a workflow B calls component A 
    """
    description = "Step (source) called by workflow (target)"
    query = """
    MATCH (cc_from:CalculationComponent {component_id: $from_component_id})
    MATCH (cc_to:CalculationComponent {component_id: $to_component_id})
    MERGE (cc_from)-[:INDIRECT_FLOW {workflow_id: $workflow_id, description: $description, data_ids: $data_ids}]->(cc_to)
    """
    session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                    description=description, data_id=data_id)
    
def create_sequential_indirect_flow(session: Session, from_component_id: str, to_component_id: str, 
                                          workflow_id: str, data_id: str):
    """
    Creates an SEQUENTIAL_INDIRECT_FLOW relationship between two CalculationComponent nodes.
    A -(SEQUENTIAL_INDIRECT_FLOW)-> B
    - Case: if C calls both and B passing an output value from A to B
        This happens when workflow C calls both A and B and an output of A is used as an input parameter of B
    """
    description = "Output of step (source) used as input for step (target)"
    query = """
    MATCH (cc_from:CalculationComponent {component_id: $from_component_id})
    MATCH (cc_to:CalculationComponent {component_id: $to_component_id})
    MERGE (cc_from)-[:SEQUENTIAL_INDIRECT_FLOW {workflow_id: $workflow_id, description: $description, data_id: $data_id}]->(cc_to)
    """
    session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                description=description, data_id=data_id)