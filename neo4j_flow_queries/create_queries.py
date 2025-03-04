import pathlib

from neo4j import Session

def create_calculation_component_node(session: Session, component_id: str, component_type: str):
    """Creates a CalculationComponent node for a given component_id if it does not exist."""
    nice_id = pathlib.Path(component_id).stem
    query = """
    MERGE (cc:CalculationComponent {component_id: $component_id})
    SET cc.nice_id = $nice_id
    SET cc.component_type = $component_type
    RETURN cc
    """
    session.run(query, component_id=component_id, nice_id=nice_id, component_type=component_type)

def create_direct_flow(session: Session, from_component_id: str, to_component_id: str, 
                             workflow_id: str, data_ids: str, workflow_list: list):
    """
    Creates a DIRECT_FLOW relationship between two CalculationComponent nodes.
    A -(DIRECT_FLOW)-> B if A calls B
    This happens when a workflow A calls component B as a step
    """
    description = "Workflow (source) calls step (target)"
    query = """
    MATCH (cc_from:CalculationComponent {component_id: $from_component_id})
    MATCH (cc_to:CalculationComponent {component_id: $to_component_id})
    MERGE (cc_from)-[r:DIRECT_FLOW 
        {workflow_id: $workflow_id, description: $description, workflow_list:apoc.coll.sort($workflow_list)}]->(cc_to)
    ON MATCH SET r.data_ids = apoc.coll.toSet(r.data_ids + $data_ids)
    ON CREATE SET r.data_ids = $data_ids
    """
    session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                description=description, data_ids=data_ids, workflow_list=workflow_list)
        
def create_indirect_flow(session: Session, from_component_id: str, to_component_id: str, 
                               workflow_id: str, data_ids: str, workflow_list: list):
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
    MERGE (cc_from)-[r:INDIRECT_FLOW 
        {workflow_id: $workflow_id, description: $description, workflow_list:apoc.coll.sort($workflow_list)}]->(cc_to)
    ON MATCH SET r.data_ids = apoc.coll.toSet(r.data_ids + $data_ids)
    ON CREATE SET r.data_ids = $data_ids
    """
    session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                    description=description, data_ids=data_ids, workflow_list=workflow_list)
    
def create_sequential_indirect_flow(session: Session, from_component_id: str, to_component_id: str, 
                                          workflow_id: str, data_ids: str, workflow_list: list):
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
    MERGE (cc_from)-[r:SEQUENTIAL_INDIRECT_FLOW 
        {workflow_id: $workflow_id, description: $description, workflow_list:apoc.coll.sort($workflow_list)}]->(cc_to)
    ON MATCH SET r.data_ids = apoc.coll.toSet(r.data_ids + $data_ids)
    ON CREATE SET r.data_ids = $data_ids
    """
    session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                description=description, data_ids=data_ids, workflow_list=workflow_list)