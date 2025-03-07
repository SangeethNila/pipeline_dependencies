import copy
from neo4j import Driver, GraphDatabase
import pandas as pd
from neo4j_dependency_queries.processing_queries import get_all_component_ids
from collections import Counter


class ChangeImpact:

    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def __init__(self, driver: Driver):
        self.driver = driver

    def close(self):
        """Closes the Neo4j database connection."""
        self.driver.close()

    def have_same_prefix(self, s1, s2):
        parts1 = s1.split("\\")
        parts2 = s2.split("\\")

        return parts1[:2] == parts2[:2] if len(parts1) >= 2 and len(parts2) >= 2 else False

    
    def complete_path_analysis(self, paths: dict[str, dict[str, list]], penalty):
        component_ids = get_all_component_ids(self.driver.session())
        sorted_component_ids = sorted([ id for id in component_ids if "example" not in id])
        print(sorted_component_ids)
        matrix = pd.DataFrame(-1.0, index=sorted_component_ids, columns=sorted_component_ids)

        for component_id_1 in sorted_component_ids:


            for component_id_2 in sorted_component_ids:

                if component_id_1 > component_id_2: continue
                if not self.have_same_prefix(component_id_1, component_id_2): continue

                paths_from_1 = component_id_1 in paths
                paths_from_2 = component_id_2 in paths

                if not paths_from_1 and not paths_from_2: continue

                print(f"Analyzing {component_id_1} and {component_id_2}")

                all_paths = list()

                if paths_from_1 and component_id_2 in paths[component_id_1]:
                    all_paths.extend(paths[component_id_1][component_id_2])

                if paths_from_2 and component_id_1 in paths[component_id_2]:
                    all_paths.extend(paths[component_id_2][component_id_1])

                distances = [path[1] for path in all_paths]
                # Get the frequency dictionary
                distance_counts = dict(Counter(distances))
                # Compute coupling score
                coupling_score = sum(N_l / (l ** penalty) for l, N_l in distance_counts.items() if l > 0)

                matrix.at[component_id_1, component_id_2] = coupling_score
                matrix.at[component_id_2, component_id_1] = coupling_score
        matrix.to_csv("change_impact_analysis.csv")

        return matrix
