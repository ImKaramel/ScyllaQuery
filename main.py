import json
import sys
import requests
from cassandra.cluster import Cluster
# path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQuery"
path = "/Users/madina/Downloads/ScyllaQuery/"

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
        rows = session.execute(f"SELECT * FROM {table_name} WHERE {field_name} >= {value} ALLOW FILTERING")
        print(rows)
        result = []
        for row in rows:
            result.append({
                "actionid": row.actionid,
                field_name: getattr(row, field_name)
            })

        with open(f"results/results{graph}/queryFilter.json", "w") as file:
            json.dump(result, file, indent=4)

        return result

    def queryFilterExtended(self, graph, table_name, result, field_name, value, degree):
        query_vertices = f"SELECT DISTINCT USERID FROM {table_name} WHERE {field_name} = {value} ALLOW FILTERING"
        rows = session.execute(query_vertices, [value])

        result_vertices = []

        # Для каждой вершины вычисление степени и фильтрация
        for row in rows:
            userid = row.userid

            # Запрос для получения количества связей для данной вершины
            query_degree = "SELECT COUNT(*) FROM actions WHERE USERID = %s"
            edge_count = session.execute(query_degree, [userid]).one().count

            # Фильтрация по степени
            if edge_count >= degree:
                result_vertices.append(userid)

        print(result_vertices)


if __name__ == "__main__":
    #config_path = sys.argv[1]

    #config_path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQuery/configs/configElliptic.json"
    # config_path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQuery/configs/configMooc.json"
    #config_path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQuery/configs/configRoadNet.json"
    #config_path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQueryconfigs/configStableCoin.json"
    config_path = "/Users/madina/Downloads/ScyllaQuery/configs/configMooc.json"

    with open(config_path, "r") as f:
        config = json.load(f)

    graph_name = config["graphName"]
    Query = ScyllaQuery()

    # with open(path + "stats/stats" + graph_name, 'w') as file:
    #     pass

    resultQueryFilter = Query.queryFilter(graph_name, config["queryFilter"]["table_name"],
                                          config["queryFilter"]["fieldName"], config["queryFilter"]["value"])

    # resultQueryFilterExtended = Query.queryFilterExtended(graph_name,
    #                                                       config["queryFilterExtended"]["collection"],
    #                                                       config["queryFilterExtended"]["edge"],
    #                                                       config["queryFilterExtended"]["fieldName"],
    #                                                       config["queryFilterExtended"]["value"],
    #                                                       config["queryFilterExtended"]["degree"])
    #
    # resultQueryBFS = Query.queryBFS(graph_name, config["queryBFS_DFS"]["depth"], config["queryBFS_DFS"]["startVertex"],
    #                                 config["queryBFS_DFS"]["fieldName"], config["queryBFS_DFS"]["value"])
    #
    # resultQueryDFS = Query.queryDFS(graph_name, config["queryBFS_DFS"]["depth"], config["queryBFS_DFS"]["startVertex"],
    #                                 config["queryBFS_DFS"]["fieldName"], config["queryBFS_DFS"]["value"])
    #
    # resultQueryFilterSum = Query.queryFilterSum(graph_name,
    #                                             config["queryFilterSum"]["collection"],
    #                                             config["queryFilterSum"]["action"],
    #                                             config["queryFilterSum"]["fieldName"],
    #                                             config["queryFilterSum"]["value"],
    #                                             config["queryFilterSum"]["sumValue"])
