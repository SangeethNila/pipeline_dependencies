from neo4j import Driver, GraphDatabase, Session
from collections import deque
import pprint
import copy
import networkx as nx
from metric_calculations.utils import append_paths_entry, current_stack_structure_processed
from neo4j_dependency_queries.processing_queries import get_all_in_parameter_nodes_of_entity, get_all_outer_out_parameter_nodes, get_all_outgoing_edges, get_data_flow_relationships_for_sorting, get_node_details, get_nodes_with_control_edges, get_valid_connections, get_workflow_list_of_data_edges_from_node, initiate_workflow_list, update_workflow_list_of_edge
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

    # Build a directed graph and perform a topological sort
    def perform_topological_sort(self,session):
        # Get the edges (relationships) from Neo4j
        edges = get_data_flow_relationships_for_sorting(session)

        # Create a directed graph
        G = nx.DiGraph()
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
            initiate_workflow_list(session)
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
        """Performs DFS traversal and stores the resulting subgraph information in Neo4j."""
        start_component_id = clean_component_id(component_id)
        
        # Find all starting nodes with the given component_id
        result = get_all_in_parameter_nodes_of_entity(session, start_component_id)
        start_nodes = [record["nodeId"] for record in result]

        for node_id in start_nodes:
            self._dfs_traverse_paths(session, node_id, deque([]), deque([]), bookkeeping)
            
    def _dfs_traverse_paths(self, session: Session, node_id: int, component_stack: deque, step_stack: deque, 
                            bookkeeping: dict[str, list[tuple[list, list]]]):
        """Recursively performs DFS traversal and saves the subgraph."""


        component_id, current_node_labels, component_type = get_node_details(session, str(node_id))

        # If an InParameter node has a new component_id, enter this component
        if "InParameter" in current_node_labels:
            component_stack.append((component_id, component_type))
            print(f"entering {component_id}")

        if not component_stack:
            return
        
        # Exit component when an OutParameter is found
        if "OutParameter" in current_node_labels:
            if component_id != component_stack[-1][0]:
                raise ValueError("Something went wrong.")
            component_stack.pop()

        if not component_stack:
            return

        # Create list versions of the current component and step stacks
        current_cs = list(component_stack)
        current_ss = list(step_stack)
        
        # Check if the current node has been encountered before
        if node_id in bookkeeping:
            # Iterate through previously stored paths that lead to this node
            if current_stack_structure_processed(bookkeeping, node_id, current_cs, current_ss):
                return
                
            bookkeeping[node_id].append((current_cs, current_ss))
        else:
            # Initialize a new entry in the bookkeeping dictionary for this node
            bookkeeping[node_id] = list()
            bookkeeping[node_id].append((current_cs, current_ss))
        
        # Find valid connections
        results = list()
        if component_stack[-1][1] == "Workflow" and step_stack and "InParameter" not in current_node_labels:
            results = get_valid_connections(session, node_id, component_stack[-1][0], step_stack[-1])
            step_stack.pop()
        else:
            results = get_valid_connections(session, node_id, component_stack[-1][0])


        records = [ (record["relId"], record["nextNodeId"], record["stepId"]) for record in results ]

        for record in records:      
    
            edge_id = record[0]   
            next_node_id = record[1]         
            step_id = record[2]
    
            edge_workflow_list = [tup[0] for tup in current_cs if tup[1] == "Workflow"]
            update_workflow_list_of_edge(session, edge_id, edge_workflow_list)

            new_component_stack = copy.deepcopy(component_stack)
            new_step_stack = copy.deepcopy(step_stack)

            if step_id != "":
                new_step_stack.append(step_id)

            # Recursively continue DFS
            self._dfs_traverse_paths(session, next_node_id, new_component_stack, new_step_stack, bookkeeping)


    def perform_change_impact(self):
        with self.driver.session() as session:
            sorted_components = self.perform_topological_sort(session)
            bookkeeping = {}
            paths = {}
            if sorted_components:
                print("Topologically Sorted Component IDs:", sorted_components)
            workflow_ids = sorted_components
            for workflow in workflow_ids:
                if 'example' in workflow:
                    print(f"Preprocessing: {workflow}")
                    self.traverse_graph_change_impact(session, workflow, bookkeeping, paths)

            pprint.pprint(paths)

    def traverse_graph_change_impact(self, session: Session, component_id: str, bookkeeping: dict, paths: dict):
        """
        Performs DFS traversal of data paths, saving direct, indirect, and sequential data flows
        between each pair of components.
        """
        start_component_id = clean_component_id(component_id)
        
        # Find all starting nodes with the given component_id
        result = get_all_in_parameter_nodes_of_entity(session, start_component_id)
        start_nodes = [record["nodeId"] for record in result]

        for node_id in start_nodes:
            self._dfs_traverse_paths_change_impact(session, node_id, deque([]), deque([]), dict(), 0, paths, bookkeeping)

    def process_sequential_flows_to_component(self, component_id: str, depth: int, last_seen: dict, outer_workflows: list, paths: dict):
        for seen_id, depth_seen in last_seen.items():
            if seen_id != component_id and seen_id not in outer_workflows:
                distance = depth - depth_seen
                for outer_component_id in outer_workflows:
                    if depth_seen > last_seen[outer_component_id]:
                        append_paths_entry(seen_id, component_id, tuple([outer_component_id, distance]), paths)
                        print(f"added path from {seen_id} to {component_id} with distance {distance} (sequential flows)")

    def process_direct_indirect_flow_of_node_id(self, node_id, component_id, outer_workflows, component_stack, step_stack, bookkeeping, paths):
        """
        Processes the direct and indirect flow of a given node within the outer workflows.

        This function iterates through the outer workflows and establishes bidirectional paths 
        between the given component and the outer workflows. If the node has 
        already been processed as a member of an outer workflow in the context of the same step(s), 
        it skips redundant processing.

        Parameters:
            node_id (str): The unique identifier of the node being processed.
            component_id (str): The identifier of the component currently being processed.
            outer_workflows (list): A list of component IDs representing outer workflows.
            component_stack (deque): A stack maintaining the sequence of outer components encountered.
            step_stack (deque): A stack maintaining the sequence of outer steps taken.
            bookkeeping (dict): A record of previously processed nodes to prevent redundant computations.
            paths (dict): A dictionary storing established connections between components.
        """
        
        for index, outer_component_id in enumerate(outer_workflows):
            # Skip if the outer component is the same as the current component
            if component_id != outer_component_id:
                # Check if the node has already been processed
                if node_id in bookkeeping:
                    # Extract the nested components and steps relevant to the current workflow depth
                    nested_components = list(component_stack)[-len(outer_workflows) + index: ]
                    nested_steps = list(step_stack)[-len(outer_workflows) + index: ]

                    # Skip processing if the current stack structure has already been handled
                    # This avoids e.g. that a workflow A that sends one data item step Y
                    # is wrongly shown to have multiple outgoing flows to Y because of nesting 
                    if current_stack_structure_processed(bookkeeping, node_id, nested_components, nested_steps):
                        continue

                entry = tuple([outer_component_id, 1])
                append_paths_entry(component_id, outer_component_id, entry, paths)
                append_paths_entry(outer_component_id, component_id, entry, paths)
                print(f"added path from {component_id} to {outer_component_id} and viceversa with distance 1 in {outer_component_id}(direct and indirect flows)")
               
    def _dfs_traverse_paths_change_impact(self, session: Session, node_id: int, component_stack: deque, step_stack: deque, 
                            last_seen: dict[str, int], depth: int, paths: dict[str, dict[str, list]], 
                            bookkeeping: dict[str, list[tuple[list, list]]]):
        """
        Recursively performs DFS traversal of data paths, saving direct, indirect, and sequential data flows
        between each pair of components.
        """

        component_id, current_node_labels, component_type = get_node_details(session, str(node_id))

        # If an InParameter node has a new component_id, enter this component
        if "InParameter" in current_node_labels:
            component_stack.append((component_id, component_type))
            print(f"entering {component_id}")

            # The first In-Parameter belongs to the outer workflow and needs depth 0
            last_seen[component_id] = depth

            outer_workflows = [workflow[0] for workflow in component_stack if workflow[1] == "Workflow"]
            self.process_sequential_flows_to_component(component_id, depth, last_seen, outer_workflows, paths)
            self.process_direct_indirect_flow_of_node_id(node_id, component_id, outer_workflows, component_stack, step_stack, bookkeeping, paths)
            
            depth = depth + 1
        
        if not component_stack: return
        
        # Exit component when an OutParameter is found
        if "OutParameter" in current_node_labels:
            component_stack.pop()
            if component_type == "Workflow":
                # When we exit a workflow, the workflow needs to be at 
                # the same depth of its last step
                last_seen[component_id] = depth - 1 

        if not component_stack: return

        # Create list versions of the current component and step stacks
        current_cs = list(component_stack)
        current_ss = list(step_stack)
        
        # Check if the current node has been encountered before
        if node_id in bookkeeping:
            # Check if the path is a (sub)path already processed
            if current_stack_structure_processed(bookkeeping, node_id, current_cs, current_ss):
                return # If so, exit early to avoid redundant computation
                
            bookkeeping[node_id].append((current_cs, current_ss))
        else:
            # Initialize a new entry in the bookkeeping dictionary for this node
            bookkeeping[node_id] = list()
            bookkeeping[node_id].append((current_cs, current_ss))

        
        # Find valid connections
        results = list()
        if component_stack[-1][1] == "Workflow" and step_stack and "InParameter" not in current_node_labels:
            results = get_valid_connections(session, node_id, component_stack[-1][0], step_stack[-1])
            step_stack.pop()
        else:
            results = get_valid_connections(session, node_id, component_stack[-1][0])

        records = [ (record["relId"], record["nextNodeId"], record["stepId"]) for record in results ]

        for record in records:      
    
            edge_id = record[0]   
            next_node_id = record[1]         
            step_id = record[2]
    
            edge_workflow_list = [tup[0] for tup in current_cs if tup[1] == "Workflow"]
            update_workflow_list_of_edge(session, edge_id, edge_workflow_list)

            new_component_stack = copy.deepcopy(component_stack)
            new_step_stack = copy.deepcopy(step_stack)
            new_last_seen = copy.deepcopy(last_seen)
            new_depth = copy.deepcopy(depth)

            if step_id != "":
                new_step_stack.append(step_id)

            # Recursively continue DFS
            self._dfs_traverse_paths_change_impact(session, next_node_id, new_component_stack, new_step_stack, 
                                                    new_last_seen, new_depth, paths, bookkeeping)
