from neo4j import Session
from neo4j_graph_queries.processing_queries import get_all_workflow_ids, get_data_flow_relationships_for_sorting
import networkx as nx

def append_info_flow_entry(id1: str, id2: str, entry: tuple[str, int], info_flows: dict[str, dict[str, list]]) -> None:
    """
    Adds an entry to the paths dictionary, ensuring necessary keys exist.

    Parameters:
        id1 (str): The first identifier key.
        id2 (str): The second identifier key, nested under id1.
        entry (tuple[str, int]): The entry to append, consisting of a string and an integer.
        paths (dict[str, dict[str, list]]): The dictionary storing path entries.
    """
    if id1 not in info_flows:
        info_flows[id1] = dict()
    if id2 not in info_flows[id1]:
        info_flows[id1][id2] = list()
    info_flows[id1][id2].append(entry)

def is_substack(inner_stack: list, outer_stack: list) -> bool:
    """
    Checks if `inner_stack` is a suffix of `outer_stack`.

    Parameters:
        inner_stack (list): The smaller stack to check.
        outer_stack (list): The larger stack where `inner_stack` might be a suffix.
    
    Returns:
        bool: True if `inner_stack` is a suffix of `outer_stack`, False otherwise.
    """
    if len(inner_stack) > len(outer_stack):
        return False
    # Check if the last elements of outer_stack match inner_stack
    return outer_stack[-len(inner_stack):] == inner_stack

def current_stack_structure_processed(bookkeeping, node_id, current_cs, current_ss) -> bool:
    """
    Checks if the given stack structure has already been processed.

    Parameters:
        bookkeeping (dict): A dictionary storing previously processed stack structures.
        node_id (str): The identifier for the node being checked.
        current_cs (list): The current component stack being processed.
        current_ss (list): The current step stack being processed.
    
    Returns:
        bool: True if the current stack structure has already been processed, False otherwise.
    """
    for existing_cs, existing_ss in bookkeeping[node_id]:
        # Check if the existing path is longer or equal to the current one
        if is_substack(current_cs, existing_cs) and is_substack(current_ss, existing_ss):
            return True
    return False

# Build a directed graph and perform a topological sort
def perform_topological_sort(session: Session):
    # Add all workflow IDs as edges
    workflow_ids = get_all_workflow_ids(session)
    # Add edge workflow_A -> workflow_B if A calls B as subworkflow
    edges = get_data_flow_relationships_for_sorting(session)

    # Create a directed graph
    G = nx.DiGraph()
    G.add_nodes_from(workflow_ids)
    G.add_edges_from(edges)
    # Perform topological sort
    try:
        sorted_components = list(nx.topological_sort(G))
        return sorted_components
    except nx.NetworkXUnfeasible:
        raise Exception("The graph is not a DAG (Directed Acyclic Graph), so a topological sort is not possible.")
    