from neo4j import Driver, GraphDatabase
import os, dotenv, pathlib

from neo4j_queries.utils import clean_component_id

class Neo4jTraversalDFS:
    """Class to perform DFS traversal and save the resulting subgraph."""

    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def __init__(self, driver: Driver):
        self.driver = driver

    def close(self):
        """Closes the Neo4j database connection."""
        self.driver.close()

    def _create_calculation_component(self, session, component_id):
        """Creates a CalculationComponent node for a given component_id if it does not exist."""
        nice_id = pathlib.Path(component_id).stem
        query = """
        MERGE (cc:CalculationComponent2 {component_id: $component_id})
        SET cc.nice_id = $nice_id
        RETURN cc
        """
        session.run(query, component_id=component_id, nice_id=nice_id)

    def _create_calculation_component_2(self, session, component_id, entity_type):
        """Creates a CalculationComponent node for a given component_id if it does not exist."""
        nice_id = pathlib.Path(component_id).stem
        query = """
        MERGE (cc:CalculationComponent2 {component_id: $component_id})
        SET cc.nice_id = $nice_id
        SET cc.entity_type = $entity_type
        RETURN cc
        """
        session.run(query, component_id=component_id, nice_id=nice_id, entity_type=entity_type)

    def _create_direct_local_flow(self, session, from_component_id, to_component_id, workflow_id):
        """
        Creates a DIRECT_LOCAL_FLOW relationship between two CalculationComponent nodes.
        A -(DIRECT_LOCAL_FLOW)-> B if A calls B
        This happens when a workflow A calls component B as a step
        """
        description = "Workflow (source) calls step (target)"
        query = """
        MATCH (cc_from:CalculationComponent {component_id: $from_component_id})
        MATCH (cc_to:CalculationComponent {component_id: $to_component_id})
        MERGE (cc_from)-[:DIRECT_LOCAL_FLOW {workflow_id: $workflow_id, description: $description}]->(cc_to)
        """
        session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                    description=description)

    def _create_direct_local_flow_2(self, session, from_component_id, to_component_id, workflow_id, data_id):
        """
        Creates a DIRECT_LOCAL_FLOW relationship between two CalculationComponent nodes.
        A -(DIRECT_LOCAL_FLOW)-> B if A calls B
        This happens when a workflow A calls component B as a step
        """
        description = "Workflow (source) calls step (target)"
        query = """
        MATCH (cc_from:CalculationComponent2 {component_id: $from_component_id})
        MATCH (cc_to:CalculationComponent2 {component_id: $to_component_id})
        MERGE (cc_from)-[:DIRECT_LOCAL_FLOW {workflow_id: $workflow_id, description: $description, data_id: $data_id}]->(cc_to)
        """
        session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                    description=description, data_id=data_id)

    def _create_indirect_local_flow(self, session, from_component_id, to_component_id, workflow_id):
        """
        Creates a DIRECT_LOCAL_FLOW relationship between two CalculationComponent nodes.
        A -(INDIRECT_LOCAL_FLOW)-> B
        - Case 1: if B calls A and A returns a value to B, which B subsequenty uses
            This happens when a workflow B calls component A 
        - Case 2: if C calls both and B passing an output value from A to B
            This happens when workflow C calls both A and B and an output of A is used as an input parameter of B
        """
        description = "Step (source) called by workflow (target)"
        if to_component_id != workflow_id:
            description = "Output of step (source) used as input for step (target)"
        query = """
        MATCH (cc_from:CalculationComponent {component_id: $from_component_id})
        MATCH (cc_to:CalculationComponent {component_id: $to_component_id})
        MERGE (cc_from)-[:INDIRECT_LOCAL_FLOW {workflow_id: $workflow_id, description: $description}]->(cc_to)
        """
        session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                    description=description)
        
    def _create_indirect_local_flow_2(self, session, from_component_id, to_component_id, workflow_id, data_id):
        """
        Creates a DIRECT_LOCAL_FLOW relationship between two CalculationComponent nodes.
        A -(INDIRECT_LOCAL_FLOW)-> B
        - Case 1: if B calls A and A returns a value to B, which B subsequenty uses
            This happens when a workflow B calls component A 
        - Case 2: if C calls both and B passing an output value from A to B
            This happens when workflow C calls both A and B and an output of A is used as an input parameter of B
        """
        description = "Step (source) called by workflow (target)"
        if to_component_id != workflow_id:
            description = "Output of step (source) used as input for step (target)"
        query = """
        MATCH (cc_from:CalculationComponent2 {component_id: $from_component_id})
        MATCH (cc_to:CalculationComponent2 {component_id: $to_component_id})
        MERGE (cc_from)-[:INDIRECT_LOCAL_FLOW {workflow_id: $workflow_id, description: $description, data_id: $data_id}]->(cc_to)
        """
        session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id,
                    description=description, data_id=data_id)
        
    def update_edge(self, session, edge_id, workflow_id):
        query = """MATCH ()-[r]->()
            WHERE elementId(r) = $edge_id
            SET r.workflow_list = 
                CASE 
                    WHEN r.workflow_list IS NULL THEN [$workflow_id]
                    WHEN NOT $workflow_id IN r.workflow_list THEN r.workflow_list + [$workflow_id]
                    ELSE r.workflow_list
                END
            RETURN r.workflow_list"""
        session.run(query, edge_id=edge_id, workflow_id=workflow_id)

    def get_all_workflow_ids(self, session):
        query = """MATCH (n:OutParameter)  
            WHERE n.entity_type="Workflow"
            RETURN COLLECT(DISTINCT n.component_id) AS component_ids
            """
        result = session.run(query)
        return result.single()["component_ids"]  # Extract the list

    def preprocess_all_graphs(self):
        with self.driver.session() as session:
            workflow_ids = self.get_all_workflow_ids(session)
            visited_workflows = set()
            for workflow in workflow_ids:
                visited_workflows.union(self.traverse_subgraph(session, workflow, calculation=False))

    def traverse_subgraph(self, session, component_id, calculation = True):
        """Performs DFS traversal and stores the resulting subgraph in Neo4j."""
        start_component_id = clean_component_id(component_id)
        
        # Find all starting nodes with the given component_id
        query = """
        MATCH (n:OutParameter {component_id: $component_id})
        RETURN elementId(n) AS nodeId, n.entity_type AS entityType
        """
        result = session.run(query, component_id=start_component_id)
        start_nodes = [record["nodeId"] for record in result]
        entity_type = [record["entityType"] for record in result]

        allowed_component_ids = {start_component_id}  # Set of allowed component_ids
        visited_nodes = {} # Track visited nodes to avoid cycles
        visited_edges = set()  # Track visited relationships

        if calculation:
            # Create CalculationComponent node for this component_id if not already created
            self._create_calculation_component_2(session, start_component_id, entity_type)

        for node_id in start_nodes:
            self._dfs_traverse(session, start_component_id, node_id, allowed_component_ids, visited_nodes, visited_edges, calculation)
            
        return allowed_component_ids

    def _dfs_traverse(self, session, workflow_id, node_id, allowed_component_ids, visited_nodes, visited_edges, calculation):
        """Recursively performs DFS traversal and saves the subgraph."""
        if node_id in visited_nodes:
            if allowed_component_ids == visited_nodes[node_id]:
                return  # Stop if already visited with same list of allowed components

        visited_nodes[node_id] = allowed_component_ids # Mark node as visited
        print(f"Visited Node ID: {node_id}")

        if calculation:

            # Get the component_id for the current node 
            query = """
            MATCH (n) WHERE elementId(n) = $node_id
            RETURN n.component_id AS component_id, labels(n) AS nodeLabels
            """
            result = session.run(query, node_id=node_id)
            record = result.single()
            component_id = record["component_id"]
            starting_node_labels = record["nodeLabels"]

        # Find valid connections
        query = """
        MATCH (n)-[r]-(m)
        WHERE elementId(n) = $node_id AND r.component_id IN $allowed_component_ids
        RETURN elementId(m) AS nextNodeId, elementId(r) AS relId, m.component_id AS nextComponentId, 
            labels(m) AS nodeLabels, type(r) AS relType, r.component_id AS relComponentId,
            r.data_id AS dataId,
            CASE WHEN m.entity_type IS NOT NULL THEN m.entity_type ELSE null END AS nextEntityType
        """
        results = session.run(query, node_id=node_id, allowed_component_ids=list(allowed_component_ids))

        for record in results:
            next_node_id = record["nextNodeId"]
            rel_id = record["relId"]
            next_component_id = record["nextComponentId"]
            node_labels = record["nodeLabels"]
            rel_component_id = record["relComponentId"]
            next_entity_type = record["nextEntityType"]
            data_id = record["dataId"]

            self.update_edge(session, rel_id, workflow_id)

            # Save the relationship if not already visited
            visited_edges.add(rel_id)

            # If an OutParameter node has a new component_id, expand allowed list
            if "OutParameter" in node_labels and next_component_id not in allowed_component_ids:
                allowed_component_ids.add(next_component_id)
                if calculation:
                    self._create_calculation_component_2(session, next_component_id, next_entity_type)
                    self._create_direct_local_flow_2(session, workflow_id, next_component_id, workflow_id, data_id)
                    self._create_indirect_local_flow_2(session, next_component_id, workflow_id, workflow_id, data_id)

                print(f"New component_id added: {next_component_id}")

            if calculation:
                # If the relation is B -> A where B is InParameter and A is OutParameter
                if "InParameter" in starting_node_labels and "OutParameter" in node_labels:
                    # Then there is a direct flow from A to B
                    self._create_indirect_local_flow_2(session, next_component_id, component_id, rel_component_id, data_id)
                    self._create_indirect_local_flow_2(session, next_component_id, component_id, workflow_id, data_id)

            # Recursively continue DFS
            self._dfs_traverse(session, workflow_id, next_node_id, allowed_component_ids, visited_nodes, visited_edges, calculation)
