from neo4j import Driver, GraphDatabase, Session
from collections import deque
import copy
from graph_analysis.utils import current_stack_structure_processed, perform_topological_sort
from neo4j_graph_queries.processing_queries import get_all_in_parameter_nodes_of_entity, get_node_details, get_nodes_with_control_edges, get_valid_connections, get_workflow_list_of_data_edges_from_node, initiate_workflow_list, update_workflow_list_of_edge
from neo4j_graph_queries.utils import clean_component_id, get_is_workflow_class

class SubgraphPreprocessing:
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
            sorted_components = perform_topological_sort(session)
            bookkeeping = {}
            if sorted_components:
                print("Topologically Sorted Component IDs:", sorted_components)
            outer_workflow_ids = sorted_components
            initiate_workflow_list(session)
            for workflow in outer_workflow_ids:
                print(f"Preprocessing: {workflow}")
                self.traverse_graph_process_paths(session, workflow, bookkeeping)
            control_tuples = get_nodes_with_control_edges(session)
            for tuple in control_tuples:
                target_id = tuple["targetId"]
                edge_component_id = tuple["componentId"]
                edge_id = tuple["edgeId"]
                result = get_workflow_list_of_data_edges_from_node(session, target_id, edge_component_id)
                workflow_lists = [record["workflow_list"] for record in result]
                if len(workflow_lists) > 0:
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
        if get_is_workflow_class(component_stack[-1][1]) and step_stack and "InParameter" not in current_node_labels:
            results = get_valid_connections(session, node_id, component_stack[-1][0], step_stack[-1])
            step_stack.pop()
        else:
            results = get_valid_connections(session, node_id, component_stack[-1][0])


        records = [ (record["relId"], record["nextNodeId"], record["stepId"]) for record in results ]

        for record in records:      
    
            edge_id = record[0]   
            next_node_id = record[1]         
            step_id = record[2]
    
            edge_workflow_list = [tup[0] for tup in current_cs if get_is_workflow_class(tup[1])]
            update_workflow_list_of_edge(session, edge_id, edge_workflow_list)

            new_component_stack = copy.deepcopy(component_stack)
            new_step_stack = copy.deepcopy(step_stack)

            if step_id != "":
                new_step_stack.append(step_id)

            # Recursively continue DFS
            self._dfs_traverse_paths(session, next_node_id, new_component_stack, new_step_stack, bookkeeping)
