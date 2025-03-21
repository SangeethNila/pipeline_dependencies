import json
from pathlib import Path
from neo4j import Driver, GraphDatabase
import pandas as pd
from neo4j_graph_queries.processing_queries import get_all_component_ids
from collections import Counter


class ChangeImpact:

    def __init__(self, uri, auth):
        """Initializes the Neo4j driver using the provided URI and authentication details."""
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def __init__(self, driver: Driver):
        """Initializes the Neo4j driver using an already established Driver object."""
        self.driver = driver

    def close(self):
        """Closes the Neo4j database connection."""
        self.driver.close()

    def have_same_repo_prefix(self, component_id_1: str, component_id_2: str):
        """
        Compares the prefixes of two component ID strings to determine if they share the same first two parts,
        which means the components belong to the same repository.

        Parameters:
            - component_id_1 (str): The first component ID string to compare.
            - component_id_2 (str): The second component ID string to compare.

        Returns:
            bool: True if the first two parts of both strings (split by '\\') are the same, False otherwise.
        """
        parts1 = component_id_1.split("\\")
        parts2 = component_id_2.split("\\")
         
        if len(parts1) < 2 or len(parts2) < 2:
            return False

        return parts1[:2] == parts2[:2]

    
    def complete_path_analysis(self, paths: dict[str, dict[str, list]]):
        """
        Analyzes the change impact between components based on the flow paths and calculates the coupling score.

        This method calculates the coupling score between components based on the distances between them
        in the flow paths, considering both direct and indirect connections. 
        The method generates a matrix where each cell (i, j) represents the coupling score between 
        components i and j. The matrix is then saved to a CSV file named `change_impact_analysis.csv`.

        Parameters:
            paths (dict[str, dict[str, list]]): A nested dictionary where the first level keys represent 
                component IDs, and the second level keys represent target component IDs. 
                The values are lists of paths from the source to the target.
                Each path in `paths[source_id][target_id]` is represented as a tuple `(context_id, distance)`, where:  
                - `context_id` is the ID of the component in whose context the path was identified.  
                - `distance` is the number of edges in the path from source to target.

        Returns:
            pd.DataFrame: A DataFrame representing the coupling score matrix between all components.

        ## Coupling Score:
            The coupling score between two components is calculated using the following formula:
            
            `coupling_score = Σ (N_l /l)` for all distinct path distances `l > 0`, where:
            
                - `N_l` is the frequency (count) of paths with distance `l`.
                - `l` is the distance (number of edges) between the two components in the flow paths.
            
            In other words, the coupling score is a weighted sum of path frequencies, where the weights are the inverses 
            of the distances raised to the power of the penalty. This formula gives more importance to shorter paths, while 
            longer paths are penalized according to the specified penalty.
        """
        component_ids = get_all_component_ids(self.driver.session())
        sorted_component_ids = sorted([ id for id in component_ids if "example" not in id])
        matrix = pd.DataFrame(-1.0, index=sorted_component_ids, columns=sorted_component_ids)

        # Loop through each pair of components
        for component_id_1 in sorted_component_ids:
            for component_id_2 in sorted_component_ids:

                # Avoid duplicate analysis
                if component_id_1 >= component_id_2: continue
                # Skip pairs with components in different repositories
                if not self.have_same_repo_prefix(component_id_1, component_id_2): continue

                # Check if paths exist from each component
                paths_from_1 = component_id_1 in paths
                paths_from_2 = component_id_2 in paths

                # Skip if no paths are available for either component
                if not paths_from_1 and not paths_from_2: continue

                # Collect all paths between component_id_1 and component_id_2
                all_paths = list()
 
                # Add paths from component_id_1 to component_id_2
                if paths_from_1 and component_id_2 in paths[component_id_1]:
                    all_paths.extend(paths[component_id_1][component_id_2])

                # Add paths from component_id_2 to component_id_1
                if paths_from_2 and component_id_1 in paths[component_id_2]:
                    all_paths.extend(paths[component_id_2][component_id_1])

                # Extract distances from the paths
                distances = [path[2] for path in all_paths]
                # Get the frequency dictionary for distances
                distance_counts = dict(Counter(distances))
                # Compute coupling score
                coupling_score = sum(N_l /l for l, N_l in distance_counts.items() if l > 0)

                # Update the matrix with the calculated coupling score for both directions
                matrix.at[component_id_1, component_id_2] = coupling_score
                matrix.at[component_id_2, component_id_1] = coupling_score

        # Save the matrix to a CSV file for further analysis
        matrix.to_csv("change_impact_analysis.csv")

        return matrix
    
    def change_impact_exploration(self, ci_matrix_path, repos):
        ci_matrix = pd.read_csv(ci_matrix_path, index_col=[0])
        files = ci_matrix.index
        cumulative_scores = {}
        for repo in repos:
            repo_path = str(Path(repo))
            cumulative_scores[repo_path] = {}
            for file in files:
                if file.startswith(repo_path):
                    cumulative_scores[repo_path][file] = ci_matrix.loc[ci_matrix[file] != -1, file].sum()

        # Save to a JSON file
        with open("change_impact_cumulative_scores.json", "w", encoding="utf-8") as file:
            json.dump(cumulative_scores, file, indent=4)
