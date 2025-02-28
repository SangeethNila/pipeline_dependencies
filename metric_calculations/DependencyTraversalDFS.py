from neo4j import Driver, GraphDatabase, Session

from neo4j_dependency_queries.processing_queries import get_all_out_parameter_nodes_of_entity, get_all_outermost_workflow_ids, get_component_id_labels_of_node, get_valid_connections, update_workflow_list_of_edge, update_workflow_list_of_node
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
        result = get_all_out_parameter_nodes_of_entity(session, start_component_id)
        start_nodes = [record["nodeId"] for record in result]
        entity_type = [record["entityType"] for record in result]

    
        called_entities = {start_component_id}  # Set of allowed component_ids
    

        if flow_graph_creation:
            # Create CalculationComponent node for this component_id if not already created
            create_calculation_component_node(session, start_component_id, entity_type)

        for node_id in start_nodes:
            allowed_component_ids = {start_component_id}  # Set of allowed component_ids
            workflow_set = {start_component_id}
            visited_nodes = {} # Track visited nodes to avoid cycles
            self._dfs_traverse(session, start_component_id, node_id, allowed_component_ids, called_entities, visited_nodes, preprocessing, flow_graph_creation, workflow_set)
            
        return called_entities

    def _dfs_traverse(self, session: Session, workflow_id: str, node_id: int, allowed_component_ids: set, called_entities: set,
                      visited_nodes:  dict[str, set], preprocess: bool, flow_graph_creation: bool, workflow_set: set = set()):
        """Recursively performs DFS traversal and saves the subgraph."""

        component_id, current_node_labels, entity_type, workflow_list = get_component_id_labels_of_node(session, node_id)

        # If an OutParameter node has a new component_id, expand allowed list
        if "OutParameter" in current_node_labels and component_id not in allowed_component_ids:

            allowed_component_ids.add(component_id)
            if entity_type == "Workflow":
                workflow_set.add(component_id)
            called_entities.add(component_id)

            print(f"New component_id added in the path: {component_id}")

        if workflow_list:
            if workflow_set.issubset(workflow_list):
                return
        update_workflow_list_of_node(session, node_id, list(workflow_set.copy()))

        

        if "InParameter" in current_node_labels and component_id  not in allowed_component_ids:
            raise ValueError()
        
        if "InParameter" in current_node_labels and component_id in allowed_component_ids:
            allowed_component_ids.remove(component_id)
            if component_id in workflow_set:
                workflow_set.remove(component_id)

        # print(f"Visited Node ID: {node_id}")

        # Find valid connections
        results = get_valid_connections(session, node_id, allowed_component_ids)

        for record in results:            
            edge_id = record["relId"]
            rel_component_id = record["relComponentId"]
            data_ids = record["dataIds"]

            next_node_id = record["nextNodeId"]
            next_component_id = record["nextComponentId"]
            next_node_labels = record["nodeLabels"]
            next_entity_type = record["nextEntityType"]
            workflow_list = record["workflowList"]

            # if workflow_list != None:
            #     if workflow_set.issubset(set(workflow_list)):
            #         continue 
            #     else: 
            #         print(f"calc: {workflow_set}")
            #         print(f"present: {workflow_set}")

            if preprocess:
                update_workflow_list_of_edge(session, edge_id, list(workflow_set.copy()))

            if flow_graph_creation:
            
                if "OutParameter" in next_node_labels:
                    create_calculation_component_node(session, next_component_id, next_entity_type)
                    create_direct_flow(session, workflow_id, next_component_id, workflow_id, data_ids, workflow_set)
                    create_indirect_flow(session, next_component_id, workflow_id, workflow_id, data_ids, workflow_set)

                    # If the relation is B -> A where B is InParameter and A is OutParameter
                    if "InParameter" in current_node_labels:
                        # Then there is a direct flow from A to B
                        create_sequential_indirect_flow(session, next_component_id, component_id, rel_component_id, data_ids, workflow_set)

            # Recursively continue DFS
            self._dfs_traverse(session, workflow_id, next_node_id, allowed_component_ids.copy(), called_entities, visited_nodes, preprocess, flow_graph_creation, workflow_set.copy())
