import csv
from collections import defaultdict
import pandas as pd
from neo4j import GraphDatabase

from neo4j_flow_queries.processing_queries import get_all_component_ids, get_indirect_flow_connections, get_outer_workflow_ids, get_sequential_indirect_flow_connections

class FlowAnalyzer:
    """Class to analyze flows between CalculationComponent nodes and export their connectivity data to CSV."""

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def __init__(self, driver):
        self.driver = driver

    def close(self):
        """Closes the Neo4j database connection."""
        self.driver.close()
    
    def dfs_path_search(self, component_id_1, component_id_2, workflow_id):
        lengths = {}
        bookkeeping = {}
        with self.driver.session() as session:
            # Start the recursive DFS from the initial component
            self.dfs(session, component_id_1, 0, 1, component_id_1, component_id_2, workflow_id, lengths, bookkeeping)
            return lengths


    def dfs(self, session, current_id, path_length, multiplicity, component_id_1, component_id_2, workflow_id, lengths, bookkeeping):
        """Recursive DFS function."""
    
        if path_length == 0:
            result = get_indirect_flow_connections(session, current_id, workflow_id)
        else:
            result = get_sequential_indirect_flow_connections(session, current_id, workflow_id)

        compressed = {}

        for record in result:
            next_component_id = record['next_component_id']
            edge_id = record['edge_id']

            if next_component_id not in compressed:
                compressed[next_component_id] = set()

            compressed[next_component_id].add(edge_id)

        if current_id in bookkeeping:
            distances = [path_length + length for length in bookkeeping[current_id]]
            for distance in distances:
                if distance not in lengths:
                    lengths[distance] = 1
                else:
                    lengths[distance] += 1


        else:
            bookkeeping[current_id] = list()

            for next_component_id, edge_ids in compressed.items():

                # Check if we reached the target component
                if next_component_id == component_id_2:
                    new_length = path_length + 1
                    bookkeeping[current_id].extend([1] * multiplicity)
                    if new_length not in lengths:
                        lengths[new_length] = multiplicity
                    else:
                        lengths[new_length] += multiplicity # Store path length
                # Check for exclusions
                elif next_component_id != workflow_id \
                    and workflow_id != component_id_1 and workflow_id != component_id_2:
                    new_multiplicity = len(edge_ids) * multiplicity
                    next_bookkeeping = self.dfs(session, next_component_id, path_length + 1, new_multiplicity, component_id_1, component_id_2, workflow_id, lengths, bookkeeping)
                    bookkeeping[current_id].extend([length + 1 for length in next_bookkeeping])  # Recursive DFS call
        return bookkeeping[current_id]


        
    def analyze_paths(self, component_id_1, component_id_2, penalty = 1.5):
        path_lengths = defaultdict(int)

        # 1. Find common workflows
        c1_workflows =  get_outer_workflow_ids(component_id_1)
        c2_workflows = get_outer_workflow_ids(component_id_2)
        common_workflows = (c1_workflows & c2_workflows)  # Intersection

        results = list()

        # 2. Find paths within each common workflow
        for workflow_id in common_workflows:
            results1 = self.dfs_path_search(component_id_1, component_id_2, workflow_id)
            results2 = self.dfs_path_search(component_id_2, component_id_1, workflow_id)
            results.extend(results1)
            results.extend(results2)
                
        for length in results:
            path_lengths[length] += 1
            

        # 3. Compute coupling score
        coupling_score = sum(N_l / (l ** penalty) for l, N_l in path_lengths.items() if l > 0)

        return {
            "path_lengths": dict(path_lengths),
            "coupling_score": coupling_score
        }
        
        
    def have_same_prefix(self, s1, s2):
        parts1 = s1.split("\\")
        parts2 = s2.split("\\")

        return parts1[:2] == parts2[:2] if len(parts1) >= 2 and len(parts2) >= 2 else False

    
    def complete_path_analysis(self):
        component_ids = get_all_component_ids(self.driver.session())
        matrix = pd.DataFrame(-1.0, index=component_ids, columns=component_ids)
        for component_id_1 in component_ids:
            print(f"Analyzing {component_id_1}")
            for component_id_2 in component_ids:
                if component_id_1 == component_id_2:
                    continue
                if not self.have_same_prefix(component_id_1, component_id_2):
                    continue
                analysis = self.analyze_paths(component_id_1, component_id_2)
                matrix.at[component_id_1, component_id_2] = analysis["coupling_score"]
        matrix.to_csv("analysis.csv")

        return matrix



    def save_to_csv(self, data, filename="calculation_component_fan_data.csv"):
        """Saves CalculationComponent fan-in, fan-out, and fan-out set data to a CSV file."""
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["component_id", "fan-in", "fan-out", "fan-out set", "fan-in * fan-out"])  # CSV Header
            for row in data:
                # Convert fan_out_set list to a string format like "[A, B, C]"
                fan_out_set_str = "[" + ", ".join(map(str, row["fan_out_set"])) + "]" if row["fan_out_set"] else "[]"
                writer.writerow([row["component_id"], row["fan_in"], row["fan_out"], fan_out_set_str, row["fan_in"] * row["fan_out"]])


