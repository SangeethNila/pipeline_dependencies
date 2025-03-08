from neo4j import Driver, GraphDatabase, Session
from collections import deque
import json
import copy
from metric_calculations.utils import append_paths_entry, current_stack_structure_processed, perform_topological_sort
from neo4j_dependency_queries.processing_queries import get_all_in_parameter_nodes_of_entity, get_node_details, get_valid_connections
from neo4j_dependency_queries.utils import clean_component_id

class FlowCalculation:
    """
    This class calculates all possible flow paths between two components and stores them in a JSON file named `flow_paths.json`.  
     
    To execute the calculation, call the `perform_flow_path_calculation` method.  

    The generated JSON is structured as a nested dictionary, where `dictionary[id1][id2]` contains a list of all paths  
    from the component with ID `id1` to the component with ID `id2`.  

    Each path is represented as a tuple `(id3, distance)`, where:  
    - `id3` is the ID of the component in whose context the path was identified.  
    - `distance` is the number of edges in the path from `id1` to `id2`.  
    """

    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def __init__(self, driver: Driver):
        self.driver = driver

    def close(self):
        """Closes the Neo4j database connection."""
        self.driver.close()

    def perform_flow_path_calculation(self):
        """
        This method performs a topological sort of components, then traverses the graph  
        to compute flow paths, storing the results in a JSON file (`flow_paths.json`).  

        ### **Data Structures:**
        - **`paths: dict[str, dict[str, list]]`**  
        - A nested dictionary where:
            - The first key (`str`) represents the source component ID.
            - The second key (`str`) represents the target component ID.
            - The value (`list`) contains all possible paths from the source to the target.  
        - This dictionary is converted into the JSON file

        - **`bookkeeping: dict[int, list[tuple[list, list]]]`**  
        - A dictionary where:
            - The key (`int`) is the **Neo4j internal node ID**.
            - The value (`list[tuple[list, list]]`) stores tuples of:
            - **`component_stack (list)`**: The workflows being traversed when the node was encountered.  
                - The **leftmost element** is the **outermost workflow**.  
            - **`step_stack (list)`**: The workflow steps being traversed when the node was encountered.    
        - This dictionary is used to avoid redundant calculations
        """
        with self.driver.session() as session:
            # Perform topological sorting to determine traversal order
            sorted_components = perform_topological_sort(session)
            bookkeeping = {}

            paths: dict[str, dict[str, list]] = {}
            workflow_ids = sorted_components
            for workflow in workflow_ids:
                print(f"Preprocessing: {workflow}")
                self.traverse_graph_change_impact(session, workflow, bookkeeping, paths)
            with open("flow_paths.json", "w") as json_file:
                json.dump(paths, json_file, indent=4)

    def traverse_graph_change_impact(self, session: Session, component_id: str, bookkeeping: dict, paths: dict[str, dict[str, list]]):
        """
        Performs a depth-first search (DFS) traversal to identify data flow paths  
        between components and updates the `paths` dictionary accordingly.

        This method initiates traversal from all "InParameter" nodes associated with  
        the specified `component_id`, detecting both direct, indirect, and sequential data flows
        between each component encountered.  

        ### **Parameters:**
        - `session (Session)`: The active Neo4j session used for querying the database.  
        - `component_id (str)`: The identifier of the component from which traversal begins.  
        - `bookkeeping (dict)`: A dictionary that tracks visited nodes and their  
        traversal states to prevent redundant processing to avoid redundant computation.  
        - `paths (dict[str, dict[str, list]]]`:
            - A nested dictionary storing discovered data flow paths.
            - Format: `paths[source_id][target_id] = list_of_paths`.  

        ### **Execution Steps:**
        1. **Sanitize `component_id`**:  
        - Calls `clean_component_id(component_id)` to standardize the ID format
            (removes local repos folder name).  

        2. **Retrieve Starting Nodes**:  
        - Queries Neo4j to find all "InParameter" nodes linked to the component.  
        - Extracts their internal Neo4j IDs into `start_nodes`.  

        3. **Initiate DFS Traversal**:  
        - Calls `_dfs_traverse_paths_change_impact` for each starting node.  
        - Initializes empty deques for `component_stack` (components currently being traversed)  
            and `step_stack` (workflow steps currently being traversed).  
        - Uses a depth counter (`depth = 0`) and an empty dictionary for  
            tracking the depths of encountered components within this traversal.  
        """
        start_component_id = clean_component_id(component_id)
        
        # Find all "InParameter" nodes associated with the component
        result = get_all_in_parameter_nodes_of_entity(session, start_component_id)
        start_nodes = [record["nodeId"] for record in result]

        # Perform DFS traversal for each starting node
        for node_id in start_nodes:
            self._dfs_traverse_paths_change_impact(session, node_id, deque([]), deque([]), dict(), 0, paths, bookkeeping)

    def _dfs_traverse_paths_change_impact(self, session: Session, node_id: int, component_stack: deque, step_stack: deque, 
                            last_seen: dict[str, int], depth: int, paths: dict[str, dict[str, list]], 
                            bookkeeping: dict[str, list[tuple[list, list]]]):
        """
        Recursively performs a depth-first search (DFS) to explore data paths and track  
        direct, indirect, and sequential data flows between components.  

        This method processes traversal by maintaining **component_stack** and **step_stack**,  
        which track the outer components and workflow steps being traversed, respectively. It updates the  
        `paths` dictionary while ensuring previously processed paths are not re-evaluated  
        using `bookkeeping`.

        ### **Parameters:**
        - `session (Session)`: The active Neo4j session for querying the database.  
        - `node_id (int)`: The Neo4j internal ID of the current node being processed.  
        - `component_stack (deque)`: A stack tracking the components currently being traversed.  
        - `step_stack (deque)`: A stack tracking workflow steps currently being traveresed.  
        - `last_seen (dict[str, int])`: Maps component IDs to the last depth they were encountered at.  
        - `depth (int)`: The current depth level in the DFS traversal.  
        - `paths (dict[str, dict[str, list]])`:  
            - A nested dictionary storing discovered flow paths.  
            - Format: `paths[source_id][target_id] = list_of_paths`.  
            - `bookkeeping (dict[str, list[tuple[list, list]]])`:  
            - Tracks previously visited nodes to prevent redundant computations.  
            - Keys are **node IDs**, values are **tuples of (component_stack, step_stack)** states.
        """

        component_id, current_node_labels, component_type = get_node_details(session, str(node_id))

        # If an InParameter node has a new component_id, enter this component
        if "InParameter" in current_node_labels:
            component_stack.append((component_id, component_type))
            print(f"entering {component_id}")

            # The first In-Parameter belongs to the outer workflow and gets depth 0
            last_seen[component_id] = depth

            # Extract list of outer workflows (leftmost = outermost)
            outer_workflows = [workflow[0] for workflow in component_stack if workflow[1] == "Workflow"]
            # Process sequential and direct/indirect flows
            self.process_sequential_flows_to_component(component_id, depth, last_seen, outer_workflows, paths)
            self.process_direct_indirect_flow_of_node_id(node_id, component_id, outer_workflows, component_stack, step_stack, bookkeeping, paths)
            
            # Increment depth as we move deeper into the traversal
            depth = depth + 1
        
        # If the stack is empty, return early
        if not component_stack: return
        
        # Exit component when an OutParameter is found
        if "OutParameter" in current_node_labels:
            component_stack.pop()
            if component_type == "Workflow":
                # When we exit a workflow, the workflow needs to be at 
                # the same depth as its last step
                last_seen[component_id] = depth - 1 

        # If the stack is empty after popping, return early
        if not component_stack: return

        # Convert current stacks into list representations for bookkeeping
        current_cs = list(component_stack)
        current_ss = list(step_stack)
        
        # Check if the current node has been encountered before
        if node_id in bookkeeping:
            # If the (sub)path structure has already been processed under the same conditions, exit early
            if current_stack_structure_processed(bookkeeping, node_id, current_cs, current_ss):
                return
            # Otherwise, update bookkeeping with the new state
            bookkeeping[node_id].append((current_cs, current_ss))
        else:
            # Initialize a new entry in bookkeeping for this node
            bookkeeping[node_id] = [(current_cs, current_ss)]
        
        # Find valid connections based on component type
        results = list()
        if component_stack[-1][1] == "Workflow" and step_stack and "InParameter" not in current_node_labels:
            # If inside a workflow and transitioning between steps, use both the top componet_id and top step_id in the stacks
            results = get_valid_connections(session, node_id, component_stack[-1][0], step_stack[-1])
            step_stack.pop()
        else:
            # Otherwise, retrieve valid connections only based on the top component_id in the component_stack
            results = get_valid_connections(session, node_id, component_stack[-1][0])

        # Extract next node IDs and step IDs from query results
        records = [ (record["nextNodeId"], record["stepId"]) for record in results ]

        # Recursively process each valid connection
        for record in records:      
            next_node_id = record[0]         
            step_id = record[1]

             # Create deep copies to ensure traversal states are independent
            new_component_stack = copy.deepcopy(component_stack)
            new_step_stack = copy.deepcopy(step_stack)
            new_last_seen = copy.deepcopy(last_seen)
            new_depth = copy.deepcopy(depth)

            # If a step ID exists, push it onto the step stack
            if step_id != "":
                new_step_stack.append(step_id)

            # Recursively continue DFS
            self._dfs_traverse_paths_change_impact(session, next_node_id, new_component_stack, new_step_stack, 
                                                    new_last_seen, new_depth, paths, bookkeeping)

    def process_sequential_flows_to_component(self, component_id: str, depth: int, last_seen: dict[str, int], outer_workflows: list, 
                                              paths: dict[str, dict[str, list]]):
        """
        Processes sequential flow paths leading to the specified component and updates the paths dictionary.  

        This method iterates through previously seen components (`last_seen`), where the keys represent encountered  
        component IDs and the values indicate the depth at which they were encountered. It calculates the distance  
        from each of these components to `component_id`.  

        If a seen component was encountered at a greater depth than an outer workflow component, a new path entry 
        from the seen component to the current component is added in the context of the outer workflow.  

        Parameters:
            component_id (str): The target component for which flow paths are being processed.  
            depth (int): The current depth in the traversal.  
            last_seen (dict): A dictionary mapping component IDs to the depth at which they were last encountered.  
            outer_workflows (list): A list of outer workflow component IDs.  
            paths (dict): A nested dictionary storing discovered flow paths.  

        Updates:
            - Adds new entries to `paths` in the format: `paths[seen_id][component_id] = (outer_component_id, distance)`.  
        """
        # Iterate through previously seen components and their recorded depths
        for seen_id, depth_seen in last_seen.items():
            # Skip the target component itself and outer workflows
            if seen_id != component_id and seen_id not in outer_workflows:
                # Calculate the distance from the seen component to the target componen
                distance = depth - depth_seen
                # Iterate through outer workflow components to determine the right context(s) of the path
                for outer_component_id in outer_workflows:
                    # Ensure the seen component was encountered at a greater depth than the outer workflow component
                    if depth_seen > last_seen[outer_component_id]:
                        append_paths_entry(seen_id, component_id, tuple([outer_component_id, distance]), paths)

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
               