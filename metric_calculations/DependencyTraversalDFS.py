from neo4j import Driver, GraphDatabase, Session

from neo4j_dependency_queries.processing_queries import get_all_outermost_workflow_ids, get_component_id_labels_of_node, get_valid_connections, update_workflow_list_of_edge
from neo4j_dependency_queries.utils import clean_component_id
from neo4j_flow_queries.create_queries import create_calculation_component_node, create_direct_flow, create_indirect_flow, create_sequential_indirect_flow
from neo4j_dependency_queries.processing_queries import get_all_workflow_ids

class DependencyTraversalDFS:
    """Class to perform DFS traversal and save the resulting subgraph."""

    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def __init__(self, driver: Driver):
        self.driver = driver

    def close(self):
        """Closes the Neo4j database connection."""
        self.driver.close()

    def preprocess_all_graphs(self):
        with self.driver.session() as session:
            outer_workflow_ids = get_all_outermost_workflow_ids(session)
            for workflow in outer_workflow_ids:
                print(f"Preprocessing: {workflow}")
                self.traverse_subgraph(session, workflow)

    def create_flow_graph(self):
        with self.driver.session() as session:
            workflow_ids = get_all_workflow_ids(session)
            visited_workflows = set()
            for workflow in workflow_ids:
                visited_workflows.union(self.traverse_subgraph(session, workflow, preprocessing=False, flow_graph_creation=True))

    def traverse_subgraph(self, session: Session, component_id: str, preprocessing: bool = True, flow_graph_creation: bool = False):
        """Performs DFS traversal and stores the resulting subgraph in Neo4j."""
        start_component_id = clean_component_id(component_id)

        if flow_graph_creation and preprocessing:
            raise ValueError("""The preprocessing cannot happen at the same time as flow calculation. 
                             First preprocess by calling 'preprocess_all_graphs', 
                             then calculate the flows by calling the function 'create_flow_graph'.""")
        
        # Find all starting nodes with the given component_id
        query = """
        MATCH (n:OutParameter {component_id: $component_id})
        RETURN elementId(n) AS nodeId, n.entity_type AS entityType
        """
        result = session.run(query, component_id=start_component_id)
        start_nodes = [record["nodeId"] for record in result]
        entity_type = [record["entityType"] for record in result]

        allowed_component_ids = {start_component_id}  # Set of allowed component_ids
        called_entities = {start_component_id}  # Set of allowed component_ids
        visited_nodes = {} # Track visited nodes to avoid cycles
        visited_edges = set()  # Track visited relationships

        if flow_graph_creation:
            # Create CalculationComponent node for this component_id if not already created
            create_calculation_component_node(session, start_component_id, entity_type)

        for node_id in start_nodes:
            self._dfs_traverse(session, start_component_id, node_id, allowed_component_ids, called_entities, visited_nodes, visited_edges, preprocessing, flow_graph_creation)
            
        print(called_entities)
        return called_entities

    def _dfs_traverse(self, session: Session, workflow_id: str, node_id: int, allowed_component_ids: set, called_entities: set,
                      visited_nodes: dict, visited_edges: set, preprocess: bool, flow_graph_creation: bool):
        """Recursively performs DFS traversal and saves the subgraph."""
        if node_id in visited_nodes:
            if allowed_component_ids == visited_nodes[node_id]:
                return  # Stop if already visited with same list of allowed components

        visited_nodes[node_id] = allowed_component_ids # Mark node as visited
        print(f"Visited Node ID: {node_id}")

        if flow_graph_creation:
            component_id, starting_node_labels = get_component_id_labels_of_node(session, node_id)

        # Find valid connections
        results = get_valid_connections(session, node_id, allowed_component_ids)

        for record in results:
            next_node_id = record["nextNodeId"]
            edge_id = record["relId"]
            next_component_id = record["nextComponentId"]
            node_labels = record["nodeLabels"]
            rel_component_id = record["relComponentId"]
            next_entity_type = record["nextEntityType"]
            data_ids = record["dataIds"]
            workflow_list = record["workflowList"]

            if flow_graph_creation and workflow_list is None:
                raise ValueError('The dependency graph has not been preprocessed. This needs to happen before flow graph calculations.')

            if preprocess:
                update_workflow_list_of_edge(session, edge_id, list(allowed_component_ids))

            # Save the relationship if not already visited
            visited_edges.add(edge_id)

            # If an OutParameter node has a new component_id, expand allowed list
            if "OutParameter" in node_labels and next_component_id not in allowed_component_ids:
                allowed_component_ids.add(next_component_id)
                called_entities.add(next_component_id)
                if flow_graph_creation:
                    create_calculation_component_node(session, next_component_id, next_entity_type)
                    create_direct_flow(session, workflow_id, next_component_id, workflow_id, data_ids, workflow_list)
                    create_indirect_flow(session, next_component_id, workflow_id, workflow_id, data_ids, workflow_list)

                print(f"New component_id added: {next_component_id}")

            if "InParameter" in node_labels and next_component_id in allowed_component_ids:
                allowed_component_ids.remove(next_component_id)

            if flow_graph_creation:
                # If the relation is B -> A where B is InParameter and A is OutParameter
                if "InParameter" in starting_node_labels and "OutParameter" in node_labels:
                    # Then there is a direct flow from A to B
                    create_sequential_indirect_flow(session, next_component_id, component_id, rel_component_id, data_ids, workflow_list)

            # Recursively continue DFS
            self._dfs_traverse(session, workflow_id, next_node_id, allowed_component_ids, called_entities, visited_nodes, visited_edges, preprocess, flow_graph_creation)
