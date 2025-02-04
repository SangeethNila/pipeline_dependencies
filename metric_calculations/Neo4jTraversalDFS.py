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
        MERGE (cc:CalculationComponent {component_id: $component_id})
        SET cc.nice_id = $nice_id
        RETURN cc
        """
        session.run(query, component_id=component_id, nice_id=nice_id)


    def _create_direct_local_flow(self, session, from_component_id, to_component_id, workflow_id):
        """
        Creates a DIRECT_LOCAL_FLOW relationship between two CalculationComponent nodes.
        A -(DIRECT_LOCAL_FLOW)-> B if A calls B
        This happens when a workflow A calls component B
        """
        query = """
        MATCH (cc_from:CalculationComponent {component_id: $from_component_id})
        MATCH (cc_to:CalculationComponent {component_id: $to_component_id})
        MERGE (cc_from)-[:DIRECT_LOCAL_FLOW {workflow_id: $workflow_id}]->(cc_to)
        """
        session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id)

    def _create_indirect_local_flow(self, session, from_component_id, to_component_id, workflow_id):
        """
        Creates a DIRECT_LOCAL_FLOW relationship between two CalculationComponent nodes.
        B -(INDIRECT_LOCAL_FLOW)-> A 
        if B calls A and A returns a value to B, which B subsequenty uses
        This happens when a workflow B calls component A 
        """
        query = """
        MATCH (cc_from:CalculationComponent {component_id: $from_component_id})
        MATCH (cc_to:CalculationComponent {component_id: $to_component_id})
        MERGE (cc_from)-[:INDIRECT_LOCAL_FLOW {workflow_id: $workflow_id}]->(cc_to)
        """
        session.run(query, from_component_id=from_component_id, to_component_id=to_component_id, workflow_id=workflow_id)

    def traverse_subgraph(self, component_id, calculation = True, visualization = False):
        """Performs DFS traversal and stores the resulting subgraph in Neo4j."""
        start_component_id = clean_component_id(component_id)
        
        with self.driver.session() as session:
            # Find all starting nodes with the given component_id
            query = """
            MATCH (n:OutParameter {component_id: $component_id})
            RETURN elementId(n) AS nodeId
            """
            start_nodes = [record["nodeId"] for record in session.run(query, component_id=start_component_id)]

            allowed_component_ids = {start_component_id}  # Set of allowed component_ids
            visited_nodes = set()  # Track visited nodes to avoid cycles
            visited_edges = set()  # Track visited relationships

            if calculation:
                # Create CalculationComponent node for this component_id if not already created
                self._create_calculation_component(session, start_component_id)

            for node_id in start_nodes:
                self._dfs_traverse(session, start_component_id, node_id, allowed_component_ids, visited_nodes, visited_edges, calculation, visualization)

    def _dfs_traverse(self, session, workflow_id, node_id, allowed_component_ids, visited_nodes, visited_edges, calculation, visualization):
        """Recursively performs DFS traversal and saves the subgraph."""
        if node_id in visited_nodes:
            return  # Stop if already visited

        visited_nodes.add(node_id)  # Mark node as visited
        print(f"Visited Node ID: {node_id}")

        if calculation:

            # Get the component_id for the current node (OutParameter)
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
        RETURN elementId(m) AS nextNodeId, elementId(r) AS relId, m.component_id AS nextComponentId, labels(m) AS nodeLabels, type(r) AS relType, r.component_id AS relComponentId
        """
        results = session.run(query, node_id=node_id, allowed_component_ids=list(allowed_component_ids))

        for record in results:
            next_node_id = record["nextNodeId"]
            rel_id = record["relId"]
            next_component_id = record["nextComponentId"]
            node_labels = record["nodeLabels"]
            rel_type = record["relType"]
            rel_component_id = record["relComponentId"]

            if visualization:
                session.run("""
                        MATCH (n)-[r]->(m)
                        WHERE elementId(n) = $node_id AND elementId(m) = $next_node_id
                        MERGE (n)-[:SUBGRAPH {original_type: $rel_type}]->(m)
                    """, node_id=node_id, next_node_id=next_node_id, rel_type=rel_type)

            # Save the relationship if not already visited
            if rel_id not in visited_edges:
                visited_edges.add(rel_id)
    
            # If an OutParameter node has a new component_id, expand allowed list
            if "OutParameter" in node_labels and next_component_id not in allowed_component_ids:
                allowed_component_ids.add(next_component_id)
                if calculation:
                    self._create_calculation_component(session, next_component_id)
                    self._create_direct_local_flow(session, workflow_id, next_component_id, workflow_id)
                    self._create_indirect_local_flow(session, next_component_id, workflow_id, workflow_id)

                print(f"New component_id added: {next_component_id}")

            if calculation:
                # If the relation is B -> A where B is InParameter and A is OutParameter
                if "InParameter" in starting_node_labels and "OutParameter" in node_labels:
                    # Then there is a direct flow from A to B
                    self._create_direct_local_flow(session, next_component_id, component_id, rel_component_id)

            # Recursively continue DFS
            self._dfs_traverse(session, workflow_id, next_node_id, allowed_component_ids, visited_nodes, visited_edges, calculation, visualization)

    def clean_up_flow(self):
        with self.driver.session() as session:
            session.run("""
                MATCH ()-[r:DIRECT_LOCAL_FLOW]-()
                DELETE r
            """)
            session.run("""
                MATCH ()-[r:INDIRECT_LOCAL_FLOW]-()
                DELETE r
            """)
            session.run("""
                MATCH ()-[r:LOCAL_FLOW]-()
                DELETE r
            """)

    def clean_up_subgraph(self):
        with self.driver.session() as session:
            session.run("""
                MATCH ()-[r:SUBGRAPH]-()
                DELETE r
            """)


# if __name__ == "__main__":
#     # Get the authentication details for Neo4j instance
#     load_status = dotenv.load_dotenv("Neo4j-25ebc0db-Created-2024-11-17.txt")
#     if load_status is False:
#         raise RuntimeError('Environment variables not loaded.')
#     URI = os.getenv("NEO4J_URI")
#     AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
#     neo4j_traversal = Neo4jTraversalDFS(URI, AUTH)

#     try:
#         start_component_id = "ldv\imaging_compress_pipeline\download_and_compress_pipeline.cwl"  # Start DFS from component_id 'x'
#         neo4j_traversal.clean_up_flow()
#         neo4j_traversal.traverse_subgraph(start_component_id)
#     finally:
#         neo4j_traversal.close()
