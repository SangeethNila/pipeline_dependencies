from pathlib import Path
from neo4j import Session

from neo4j_graph_queries.processing_queries import count_nodes_and_edges
from neo4j_graph_queries.utils import clean_component_id


def get_graph_size_per_repo(session: Session, repo_list: list[str]):
    graph_size = {}
    total_nodes = 0
    total_edges = 0
    for repo in repo_list:
        repo_path = clean_component_id(repo)
        repo_path = str(Path(repo))

        nodes, edges = count_nodes_and_edges(session ,repo_path)
        total_nodes += nodes
        total_edges += edges
        graph_size[repo_path] = {'node_count': nodes, 'edge_count': edges}
    graph_size['total'] = {'node_count': total_nodes, 'edge_count': total_edges}
    return graph_size