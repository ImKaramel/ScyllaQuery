import json
import sys
import requests
from cassandra.cluster import Cluster
path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQuery"

cluster = Cluster(contact_points=['localhost'], port=9042)
session = cluster.connect('scylla')

class ScyllaQuery:
    def __init__(self):
        pass

    def getStats(self, graph, nameQuery, execution_time, memory_usage):
        with open(path + "stats/stats" + graph, "a") as file:
            file.write(nameQuery + "\n")
            file.write("executionTime ")
            file.write(str(execution_time) + " s" + "\n")
            file.write("peakMemoryUsage " + str(memory_usage) + " byte" + "\n\n\n")

    def queryFilter(self, graph, table_name, field_name, value):

        # start_time = time.time()

        rows = session.execute(f"SELECT * FROM {table_name} WHERE {field_name} >= {value}")
        result = []
        for row in rows:
            result.append({
                "id": row.id,
                field_name: getattr(row, field_name)
            })

        with open(f"results/results{graph}/queryFilter.json", "w") as file:
            json.dump(result, file, indent=4)

        return result


# Пример использования
if __name__ == "__main__":
    query = ScyllaQuery()
    result = query.queryFilter("graph_name", "your_table_name", "your_field_name", 10)
    print(result)

    session.shutdown()
    cluster.shutdown()