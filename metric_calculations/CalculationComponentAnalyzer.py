import csv
from neo4j import GraphDatabase

class CalculationComponentAnalyzer:
    """Class to analyze CalculationComponent nodes and export their connectivity data to CSV."""

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def __init__(self, driver):
        self.driver = driver

    def close(self):
        """Closes the Neo4j database connection."""
        self.driver.close()

    def fetch_calculation_component_fan_data(self):
        """Queries Neo4j for CalculationComponent fan-in, fan-out, and fan-out set."""
        query = """
        MATCH (cc:CalculationComponent)
        OPTIONAL MATCH (cc)<-[r_in]-()  // Count all incoming relationships
        OPTIONAL MATCH (cc)-[r_out]->(target)  // Count all outgoing relationships & get targets
        RETURN cc.component_id AS component_id, 
               COUNT(DISTINCT r_in) AS fan_in, 
               COUNT(DISTINCT r_out) AS fan_out,
               COLLECT(DISTINCT target.component_id) AS fan_out_set
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            return [{"component_id": record["component_id"], 
                     "fan_in": record["fan_in"], 
                     "fan_out": record["fan_out"], 
                     "fan_out_set": record["fan_out_set"]} 
                    for record in result]

    def save_to_csv(self, data, filename="calculation_component_fan_data.csv"):
        """Saves CalculationComponent fan-in, fan-out, and fan-out set data to a CSV file."""
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["component_id", "fan-in", "fan-out", "fan-out set", "fan-in * fan-out"])  # CSV Header
            for row in data:
                # Convert fan_out_set list to a string format like "[A, B, C]"
                fan_out_set_str = "[" + ", ".join(map(str, row["fan_out_set"])) + "]" if row["fan_out_set"] else "[]"
                writer.writerow([row["component_id"], row["fan_in"], row["fan_out"], fan_out_set_str, row["fan_in"] * row["fan_out"]])


