import json
import sys
import tracemalloc
from collections import defaultdict, deque

from cassandra.cluster import Cluster
import time
import networkx as nx
# path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQuery/"
path = "/Users/madina/Downloads/ScyllaQuery/"

cluster = Cluster(contact_points=['localhost'], port=9042)
session = cluster.connect('scylla')

leaf_nodes = []
class ScyllaQuery:
    def __init__(self):
        pass

    def getStats(self, graph, nameQuery, execution_time, memory_usage):
        with open(path + "stats/stats" + graph, "a") as file:
            file.write(nameQuery + "\n")
            file.write("executionTime ")
            file.write(str(execution_time) + " s" + "\n")
            file.write("peakMemoryUsage " + str(memory_usage) + " KB" + "\n\n\n")

    def queryFilter(self, graph, table_name, id, field_name, value):
        start_time = time.time()
        tracemalloc.start()

        rows = session.execute(f"SELECT * FROM {table_name} WHERE {field_name} = {value} ALLOW FILTERING")

        end_time = time.time()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')

        self.getStats(graph, "queryFilter", end_time - start_time, top_stats[0].size / 1024 )

        result = []
        for row in rows:
            # print(result)
            result.append({
                "id": getattr(row, id),
                field_name: getattr(row, field_name)
            })

        with open(f"results/results{graph}/queryFilter.json", "w") as file:
            json.dump(result, file, indent=4)

        return result

    def queryFilterExtended(self, graph, table_name, result, degree, field_name, value):
        if graph == "RoadNet" or "Elliptic":
            start_time = time.time()
            tracemalloc.start()

            query_vertices = f"SELECT {result} FROM {table_name} WHERE {field_name} >= {value} ALLOW FILTERING"
            rows = session.execute(query_vertices)

            vertex_degrees = {}
            for row in rows:
                userid = getattr(row, result)
                query_degree = f"SELECT COUNT(*) FROM {table_name} WHERE {result} = {userid} ALLOW FILTERING"
                edge_count = session.execute(query_degree).one().count  # Подсчитываем ребра
                vertex_degrees[userid] = edge_count
                # print(vertex_degrees)
            end_time = time.time()
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')

            self.getStats(graph, "queryFilterExtended", end_time - start_time, top_stats[0].size / 1024 )

            result_vertices = [
                {'vertex': userid, 'degree': edge_count}
                for userid, edge_count in vertex_degrees.items()
                if edge_count == degree
            ]
            # print(result_vertices)

            with open(f"results/results{graph}/queryFilterExtended.json", "w") as file:
                json.dump(result_vertices, file, indent=4)

            return result_vertices
        else:
            start_time = time.time()
            tracemalloc.start()
            query_vertices = f"SELECT {result} FROM {table_name} WHERE {field_name} = {value} ALLOW FILTERING"
            rows = session.execute(query_vertices)
            print(rows)

            vertex_degrees = {}
            for row in rows:
                userid = getattr(row, result)
                query_degree = f"SELECT COUNT(*) FROM {table_name} WHERE {result} = {userid} ALLOW FILTERING"
                edge_count = session.execute(query_degree).one().count  # Подсчитываем ребра
                vertex_degrees[userid] = edge_count
                print(vertex_degrees)
            end_time = time.time()
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')

            self.getStats(graph, "queryFilterExtended", end_time - start_time, top_stats[0].size / 1024 )

            result_vertices = [
                {'vertex': userid, 'degree': edge_count}
                for userid, edge_count in vertex_degrees.items()
                if edge_count >= degree
            ]


            with open(f"results/results{graph}/queryFilterExtended.json", "w") as file:
                json.dump(result_vertices, file, indent=4)

            return result_vertices



    def queryDFS(self, graph, table_name, start_node, depth, fieldName, value,  from_id, to_id):
            visited = set()
            stack = [(start_node, 0)]

            start_time = time.time()
            tracemalloc.start()

            while stack:
                node, level = stack.pop()

                if level <= depth:
                    visited.add(node)
                    # print(visited)
                    if graph == "RoadNet" or "Elliptic":
                        cql_query = f"SELECT {to_id} FROM {table_name} WHERE {from_id} = '{node}' ALLOW FILTERING"
                    else:
                        cql_query = f"SELECT {to_id} FROM {table_name} WHERE {from_id} = '{node}' AND {fieldName} > {value} ALLOW FILTERING"
                    result = session.execute(cql_query)
                    # print(visited)
                    for row in result:
                        tonodeid = getattr(row, to_id)
                        if tonodeid not in visited:
                            stack.append((tonodeid, level + 1))

            end_time = time.time()
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')

            visited.discard(start_node)

            results = {"leaf_nodes": list(visited)}
            with open(f"results/results{graph}/queryDFS.json", "w") as file:
                json.dump(results, file, indent=4)

            self.getStats(graph, "queryDFS", end_time - start_time, top_stats[0].size / 1024  )
            return visited

    def queryBFS(self, graph, table_name, start_node, depth, fieldName, value, from_id, to_id):
            visited = set()
            queue = [(start_node, 0)]

            start_time = time.time()
            tracemalloc.start()

            while queue:
                node, level = queue.pop(0)

                if level <= depth:
                    visited.add(node)
                    # print(visited)
                    if graph == "RoadNet" or "Elliptic":
                        cql_query = f"SELECT {to_id} FROM {table_name} WHERE {from_id} = '{node}' ALLOW FILTERING"
                    else:
                        cql_query = f"SELECT {to_id} FROM {table_name} WHERE {from_id} = '{node}' AND {fieldName} > {value} ALLOW FILTERING"

                    result = session.execute(cql_query)
                    for row in result:
                        tonodeid = getattr(row, to_id)
                        if tonodeid not in visited:
                            queue.append((tonodeid, level + 1))

            end_time = time.time()
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')

            visited.discard(start_node)

            results = {"leaf_nodes": list(visited)}
            with open(f"results/results{graph}/queryBFS.json", "w") as file:
                json.dump(results, file, indent=4)

            self.getStats(graph, "queryBFS", end_time - start_time, top_stats[0].size / 1024)

    def queryFilterSum(self, graph, table_name, collection,fieldName, sumValue, value):
        if graph == "RoadNet":
            pass
        else:
            start_time = time.time()
            tracemalloc.start()

            query = f"""
                SELECT {collection}, {fieldName}
                FROM {table_name}
                WHERE {fieldName} > %s ALLOW FILTERING
            """
            rows = session.execute(query, (value,))
            # print(rows)

            user_sums = {}

            for row in rows:
                userid =  getattr(row, collection)
                timestamp = getattr(row, fieldName)
                if userid not in user_sums:
                    user_sums[userid] = 0.0
                user_sums[userid] += timestamp
                # print(user_sums)

            result = [
                {'vertex': userid, 'sum': total_sum}
                for userid, total_sum in user_sums.items()
                if total_sum > sumValue
            ]
            with open(f"results/results{graph}/queryFilterSum.json", "w") as file:
                json.dump(result, file, indent=4)
            end_time = time.time()
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')

            self.getStats(graph, "queryFilterSum", end_time - start_time, top_stats[0].size / 1024 )

            # print(result)
            return result


    def queryTriangles(self, graph, table_name, fieldName, fieldName2):
        start_time = time.time()
        tracemalloc.start()

        query = f"SELECT {fieldName}, {fieldName2} FROM {table_name}"
        edges = session.execute(query)

        edges_list = [(getattr(row, fieldName), getattr(row, fieldName2)) for row in edges]

        edge_set = set(edges_list)

        adjacency_dict = {}
        for (a, b) in edges_list:
            if a not in adjacency_dict:
                adjacency_dict[a] = set()
            if b not in adjacency_dict:
                adjacency_dict[b] = set()
            adjacency_dict[a].add(b)
            adjacency_dict[b].add(a)

        triangles = set()
        for a in adjacency_dict:
            neighbors_a = adjacency_dict[a]
            for b in neighbors_a:
                if b > a:
                    neighbors_b = adjacency_dict[b]
                    common_neighbors = neighbors_a.intersection(neighbors_b)
                    for c in common_neighbors:
                        if c > b:
                            triangle = tuple(sorted([a, b, c]))
                            triangles.add(triangle)

        # Печать найденных треугольников
        # if not triangles:
        #     print("Треугольники не найдены.")
        # else:
        #     for triangle in triangles:
        #         print(f'Triangle: {triangle}')


        end_time = time.time()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')

        self.getStats(graph, "queryTriangles", end_time - start_time, top_stats[0].size / 1024)

        triangle_list = [{"triangle": list(triangle)} for triangle in triangles]
        triangle_count = len(triangles)
        result = {
            "triangle_count": triangle_count,
            "triangles": triangle_list
        }

        with open(f"results/results{graph}/queryTriangles.json", "w") as file:
            json.dump(result, file, indent=4)

        # return len(triangles), triangles

    def queryShortPath(self, graph, table_name, fromVertex, value1, toVertex, value2):
        query = f"SELECT {fromVertex}, {toVertex} FROM {table_name}"
        rows = session.execute(query)
        start_time = time.time()
        tracemalloc.start()
        dict = defaultdict(list)
        for row in rows:
            source = getattr(row, fromVertex)
            target = getattr(row, toVertex)
            dict[source].append(target)

        queue = deque([[value1]])
        visited = set()
        paths = {value1: [value1]}

        while queue:
            path = queue.popleft()
            vertex = path[-1]

            if vertex in visited:
                continue

            for neighbor in dict[vertex]:
                if neighbor not in visited:
                    new_path = list(path)
                    new_path.append(neighbor)
                    queue.append(new_path)
                    paths[neighbor] = new_path

                    if neighbor == value2:
                        result = {'path': new_path}
                        end_time = time.time()
                        snapshot = tracemalloc.take_snapshot()
                        top_stats = snapshot.statistics('lineno')
                        self.getStats(graph, "queryShortPath", end_time - start_time, top_stats[0].size / 1024 )


                        with open(f"results/results{graph}/queryShortPath.json", 'w') as file:
                            json.dump(result, file, indent=4)

                        return result

            visited.add(vertex)
        result = {'path': None}
        with open(f"results/results{graph}/queryShortPath.json", "w") as file:
            json.dump(result, file, indent=4)
        end_time = time.time()
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')

        self.getStats(graph, "queryShortPath", end_time - start_time, top_stats[0].size / 1024 )
        return result




if __name__ == "__main__":
    # config_path = sys.argv[1]

    # config_path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQuery/configs/configElliptic.json"
    # config_path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQuery/configs/configMooc.json"
    #config_path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQuery/configs/configRoadNet.json"
    # config_path = "/Users/assistentka_professora/Desktop/Scylla/ScyllaQueryconfigs/configStableCoin.json"
    # config_path = "/Users/madina/Downloads/ScyllaQuery/configs/configMooc.json"
    config_path = "/Users/madina/Downloads/ScyllaQuery/configs/configRoadNet.json"
    # config_path = "/Users/madina/Downloads/ScyllaQuery/configs/configElliptic.json"
    # config_path = "/Users/madina/Downloads/ScyllaQuery/configs/configStableCoin.json"
    with open(config_path, "r") as f:
        config = json.load(f)

    graph_name = config["graphName"]
    Query = ScyllaQuery()

    # with open(path + "stats/stats" + graph_name, 'w') as file:
    #     pass


    # resultQueryFilter = Query.queryFilter(graph_name, config["queryFilter"]["table_name"],
    #                                       config["queryFilter"]["id"],
    #                                       config["queryFilter"]["fieldName"],
    #                                       config["queryFilter"]["value"])


    #
    # resultQueryFilterExtended = Query.queryFilterExtended(graph_name,
    #                                                       config["queryFilterExtended"]["table_name"],
    #                                                       config["queryFilterExtended"]["result"],
    #                                                       config["queryFilterExtended"]["degree"],
    #                                                       config["queryFilterExtended"]["fieldName"],
    #                                                       config["queryFilterExtended"]["value"])

    # resultQueryFilter = Query.queryFilter(graph_name, config["queryFilter"]["table_name"],
    #                                       config["queryFilter"]["id"],
    #                                       config["queryFilter"]["fieldName"], config["queryFilter"]["value"])
    # #
    # resultQueryFilterExtended = Query.queryFilterExtended(graph_name, config["queryFilterExtended"]["table_name"],
    #                                                       config["queryFilterExtended"]["table_name2"],
    #                                                       config["queryFilterExtended"]["result"],
    #                                                       config["queryFilterExtended"]["degree"],
    #                                                       config["queryFilterExtended"]["fieldName"],
    #                                                       config["queryFilterExtended"]["value"])
    #

    # resultQueryBFS = Query.queryBFS(graph_name, config["queryBFS_DFS"]["table_name"], config["queryBFS_DFS"]["startVertex"], config["queryBFS_DFS"]["depth"],
    #                                 config["queryBFS_DFS"]["fieldName"], config["queryBFS_DFS"]["value"], config["queryBFS_DFS"]["from_id"],
    #                                       config["queryBFS_DFS"]["to_id"])
    #
    # resultQueryDFS = Query.queryDFS(graph_name, config["queryBFS_DFS"]["table_name"], config["queryBFS_DFS"]["startVertex"], config["queryBFS_DFS"]["depth"],
    #                                 config["queryBFS_DFS"]["fieldName"], config["queryBFS_DFS"]["value"], config["queryBFS_DFS"]["from_id"],
    #                                       config["queryBFS_DFS"]["to_id"])



    # resultQueryFilterSum = Query.queryFilterSum(graph_name,
    #                                             config["queryFilterSum"]["table_name"],
    #                                             config["queryFilterSum"]["collection"],
    #                                             config["queryFilterSum"]["fieldName"],
    #                                             config["queryFilterSum"]["sumValue"],
    #                                             config["queryFilterSum"]["value"])


    resultQueryTriangles = Query.queryTriangles(graph_name, config["queryTriangles"]["table_name"],
                                          config["queryTriangles"]["fieldName"],
                                          config["queryTriangles"]["fieldName2"])


    # resultQueryShortPath = Query.queryShortPath(graph_name,
    #                                     config["queryShortPath"]["table_name"],
    #                                     config["queryShortPath"]["fromVertex"],
    #                                     config["queryShortPath"]["value1"],
    #                                     config["queryShortPath"]["toVertex"],
    #                                     config["queryShortPath"]["value2"],)

