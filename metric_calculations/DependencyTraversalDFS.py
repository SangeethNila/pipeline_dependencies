from neo4j import Driver, GraphDatabase, Session
from collections import deque
import copy
import networkx as nx
from neo4j_dependency_queries.processing_queries import get_all_in_parameter_nodes_of_entity, get_all_outer_out_parameter_nodes, get_all_outgoing_edges, get_node_details, get_nodes_with_control_edges, get_valid_connections, get_workflow_list_of_data_edges_from_node, instantiate_workflow_list, update_workflow_list_of_edge
from neo4j_dependency_queries.utils import clean_component_id
from neo4j_flow_queries.create_queries import create_calculation_component_node, create_direct_flow, create_indirect_flow, create_sequential_indirect_flow

class DependencyTraversalDFS:
    """Class to perform DFS traversal and save the resulting subgraph."""

    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def __init__(self, driver: Driver):
        self.driver = driver

    def close(self):
        """Closes the Neo4j database connection."""
        self.driver.close()

    def get_data_flow_relationships(self, session):
        query = """
        MATCH (a:InParameter)-[:DATA_FLOW]->(b:InParameter)
        RETURN a.component_id AS componentA, b.component_id AS componentB
        """
        result = session.run(query)
        edges = [(record['componentA'], record['componentB']) for record in result]
        return edges

    # Build a directed graph and perform a topological sort
    def perform_topological_sort(self,session):
        # Get the edges (relationships) from Neo4j
        edges = self.get_data_flow_relationships(session)

        # Create a directed graph
        G = nx.DiGraph()
        
        # Add edges to the graph
        G.add_edges_from(edges)

        # Perform topological sort
        try:
            sorted_components = list(nx.topological_sort(G))
            return sorted_components
        except nx.NetworkXUnfeasible:
            raise Exception("The graph is not a DAG (Directed Acyclic Graph), so a topological sort is not possible.")

    def preprocess_all_graphs(self):
        with self.driver.session() as session:
            sorted_components = self.perform_topological_sort(session)
            bookkeeping = {}
            if sorted_components:
                print("Topologically Sorted Component IDs:", sorted_components)
            outer_workflow_ids = sorted_components
            instantiate_workflow_list(session)
            for workflow in outer_workflow_ids:
                print(f"Preprocessing: {workflow}")
                self.traverse_graph_process_paths(session, workflow, bookkeeping)
            control_pairs = get_nodes_with_control_edges(session)
            for pair in control_pairs:
                target_id = pair["targetId"]
                edge_component_id = pair["componentId"]
                edge_id = pair["edgeId"]
                result = get_workflow_list_of_data_edges_from_node(session, target_id, edge_component_id)
                workflow_lists = [record["workflow_list"] for record in result]
                update_workflow_list_of_edge(session, edge_id, workflow_lists[0])


    def traverse_graph_process_paths(self, session: Session, component_id: str, bookkeeping):
        """Performs DFS traversal and stores the resulting subgraph in Neo4j."""
        start_component_id = clean_component_id(component_id)
        
        # Find all starting nodes with the given component_id
        result = get_all_in_parameter_nodes_of_entity(session, start_component_id)
        start_nodes = [record["nodeId"] for record in result]

        for node_id in start_nodes:
            workflow_set = {start_component_id}
            self._dfs_traverse_paths(session, node_id, workflow_set,deque([]), bookkeeping)
            
    def _dfs_traverse_paths(self, session: Session, node_id: int, workflow_set: set, entity_queue: deque, bookkeeping: dict[str, list]):
        """Recursively performs DFS traversal and saves the subgraph."""

        component_id, current_node_labels, component_type = get_node_details(session, node_id)

        # If an InParameter node has a new component_id, enter this component
        if "InParameter" in current_node_labels:
            entity_queue.append(component_id)
            if component_type == "Workflow":
                workflow_set.add(component_id)

        if not entity_queue:
            return
        
        # Exit component when an OutParameter is found
        if "OutParameter" in current_node_labels:
            if component_id != entity_queue[-1]:
                raise ValueError("Something went wrong.")
            entity_queue.pop()
            if component_id in workflow_set:
                workflow_set.remove(component_id)

        if not entity_queue:
            return
        
        current_list = list(entity_queue)
        if node_id in bookkeeping:
            for existing_list in bookkeeping[node_id]:
                if len(existing_list) >= len(current_list):
                    if existing_list[-len(current_list):] == current_list:
                        return
                
            bookkeeping[node_id].append(current_list)
        else:
            bookkeeping[node_id] = list()
            bookkeeping[node_id].append(current_list)

        
        # Find valid connections
        results = get_valid_connections(session, node_id, entity_queue[-1])
        records = [ (record["relId"], record["nextNodeId"]) for record in results]

        for record in records:            
            edge_id = record[0]
            edge_workflow_set = workflow_set.copy()

            next_node_id = record[1]

            update_workflow_list_of_edge(session, edge_id, sorted(list(edge_workflow_set)))
            new_queue = copy.deepcopy(entity_queue)
            # Recursively continue DFS
            self._dfs_traverse_paths(session, next_node_id, edge_workflow_set, new_queue, bookkeeping)

    def traverse_graph_create_flows(self):
        with self.driver.session() as session:
            # Find all starting nodes with the given component_id
            result = get_all_outer_out_parameter_nodes(session)
            start_nodes = [record["nodeId"] for record in result]
            visited_nodes = set()
            entities = set()

            for node_id in start_nodes:
                self._dfs_traverse(session, node_id, visited_nodes, entities)

    def _dfs_traverse(self, session: Session, node_id: int, visited_nodes: set, entities: set):
        """Recursively performs DFS traversal and saves the subgraph."""

        component_id, current_node_labels, component_type, workflow_list = get_node_details(session, node_id)
        if node_id in visited_nodes:
            return
        print(f"visiting {node_id}")
        
        visited_nodes.add(node_id)
        
        if component_id not in entities:
            entities.add(component_id)
            create_calculation_component_node(session, component_id, component_type)
        

        # Find all connections
        results = get_all_outgoing_edges(session, node_id)

        for record in results:            
            edge_component_id = record["relComponentId"]
            workflow_list = record["workflowList"]
            data_ids = record["dataIds"]

            next_node_id = record["nextNodeId"]
            next_component_id = record["nextComponentId"]
            next_node_labels = record["nextNodeLabels"]
            next_component_type = record["nextEntityType"]

            if workflow_list:
                workflow_list = list(workflow_list)
            else:
                workflow_list = []

            if edge_component_id not in entities:
                entities.add(component_id)
                create_calculation_component_node(session, component_id, component_type)


            
            if "InParameter" in next_node_labels:
                create_calculation_component_node(session, next_component_id, next_component_type)
                create_direct_flow(session, edge_component_id, next_component_id, edge_component_id, data_ids, workflow_list)
                create_indirect_flow(session, next_component_id, edge_component_id, edge_component_id, data_ids, workflow_list)

                if "OutParameter" in current_node_labels:
                    create_sequential_indirect_flow(session, component_id, next_component_id, edge_component_id, data_ids, workflow_list)

            # Recursively continue DFS
            self._dfs_traverse(session, next_node_id, visited_nodes, entities)
