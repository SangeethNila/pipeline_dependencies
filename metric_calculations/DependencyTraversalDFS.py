from neo4j import Driver, GraphDatabase, Session

from neo4j_dependency_queries.processing_queries import get_all_in_parameter_nodes_of_entity, get_all_out_parameter_nodes_of_entity, get_all_outer_out_parameter_nodes, get_all_outgoing_edges, get_node_details, get_nodes_with_control_edges, get_valid_connections, get_workflow_list_of_data_edge, update_workflow_list_of_edge, update_workflow_list_of_node
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

    def preprocess_all_graphs(self):
        with self.driver.session() as session:
            # outer_workflow_ids = get_all_outermost_workflow_ids(session)
            # for workflow in outer_workflow_ids:
            #     print(f"Preprocessing: {workflow}")
            #     self.traverse_graph_process_paths(session, workflow)
            control_pairs = get_nodes_with_control_edges(session)
            for pair in control_pairs:
                source_id = pair["sourceId"]
                target_id = pair["targetId"]
                edge_component_id = pair["componentId"]
                edge_id = pair["edgeId"]
                workflow_list = get_workflow_list_of_data_edge(session, source_id, target_id, edge_component_id)
                update_workflow_list_of_edge(session, edge_id, workflow_list)


    def traverse_graph_process_paths(self, session: Session, component_id: str):
        """Performs DFS traversal and stores the resulting subgraph in Neo4j."""
        start_component_id = clean_component_id(component_id)
        
        # Find all starting nodes with the given component_id
        result = get_all_in_parameter_nodes_of_entity(session, start_component_id)
        start_nodes = [record["nodeId"] for record in result]

        for node_id in start_nodes:
            allowed_component_ids = {start_component_id}  # Set of allowed component_ids
            workflow_set = {start_component_id}
            self._dfs_traverse_paths(session, start_component_id, node_id, allowed_component_ids, workflow_set)
            
    def _dfs_traverse_paths(self, session: Session, workflow_id: str, node_id: int, allowed_component_ids: set, workflow_set: set):
        """Recursively performs DFS traversal and saves the subgraph."""

        component_id, current_node_labels, entity_type, workflow_list = get_node_details(session, node_id)

        # If an InParameter node has a new component_id, expand allowed list
        if "InParameter" in current_node_labels and component_id not in allowed_component_ids:

            allowed_component_ids.add(component_id)
            if entity_type == "Workflow":
                workflow_set.add(component_id)

            print(f"New component_id added in the path: {component_id}")

        if workflow_list:
            if workflow_set.issubset(workflow_list):
                return
        update_workflow_list_of_node(session, node_id, list(workflow_set.copy()))

        
        if "OutParameter" in current_node_labels and component_id in allowed_component_ids:
            allowed_component_ids.remove(component_id)
            if component_id in workflow_set:
                workflow_set.remove(component_id)

        # Find valid connections
        results = get_valid_connections(session, node_id, allowed_component_ids)

        for record in results:            
            edge_id = record["relId"]
            next_node_id = record["nextNodeId"]

            update_workflow_list_of_edge(session, edge_id, list(workflow_set.copy()))
            # Recursively continue DFS
            self._dfs_traverse_paths(session, workflow_id, next_node_id, allowed_component_ids.copy(), workflow_set.copy())

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

        component_id, current_node_labels, entity_type, workflow_list = get_node_details(session, node_id)
        if node_id in visited_nodes:
            return
        print(f"visiting {node_id}")
        
        visited_nodes.add(node_id)
        
        if component_id not in entities:
            entities.add(component_id)
            create_calculation_component_node(session, component_id, entity_type)
        

        # Find all connections
        results = get_all_outgoing_edges(session, node_id)

        for record in results:            
            edge_component_id = record["relComponentId"]
            workflow_list = record["workflowList"]
            data_ids = record["dataIds"]

            next_node_id = record["nextNodeId"]
            next_component_id = record["nextComponentId"]
            next_node_labels = record["nextNodeLabels"]
            next_entity_type = record["nextEntityType"]

            if workflow_list:
                workflow_list = list(workflow_list)
            else:
                workflow_list = []

            if edge_component_id not in entities:
                entities.add(component_id)
                create_calculation_component_node(session, component_id, entity_type)


            
            if "InParameter" in next_node_labels:
                create_calculation_component_node(session, next_component_id, next_entity_type)
                create_direct_flow(session, edge_component_id, next_component_id, edge_component_id, data_ids, workflow_list)
                create_indirect_flow(session, next_component_id, edge_component_id, edge_component_id, data_ids, workflow_list)

                if "OutParameter" in current_node_labels:
                    create_sequential_indirect_flow(session, component_id, next_component_id, edge_component_id, data_ids, workflow_list)

            # Recursively continue DFS
            self._dfs_traverse(session, next_node_id, visited_nodes, entities)
