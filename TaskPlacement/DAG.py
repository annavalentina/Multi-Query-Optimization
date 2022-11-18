# -------------------------------------------#
# Author: Michailidou Anna-Valentini
# Part of the code taken from: https://www.geeksforgeeks.org/find-paths-given-source-destination/
# Description:File that implements creating
#   and managing Directed Acyclic Graphs and Tasks
# -------------------------------------------#

import random
import variables
from collections import Counter


# This class represents the tasks of a DAG
class Task:
    def __init__(self, task_id, task_type, cpu_req, ram_req, selectivity, input_rate, output_rate, parents):
        self.task_id = task_id
        self.taskType = task_type
        self.cpu_req = cpu_req
        self.ram_req = ram_req
        self.selectivity = selectivity
        self.input_rate = input_rate
        self.output_rate = output_rate
        self.parents = parents


# This class represents a DAG
class Graph:
    def __init__(self, mobile_device_id, graph_id, number_of_nodes, graph_dict, source, sink, tasks, paths):
        self.mobile_device_id = mobile_device_id
        self.number_of_nodes = number_of_nodes
        self.graph_id = graph_id
        self.source = source
        self.paths = paths
        self.graph_dict = graph_dict
        self.tasks = tasks
        self.sink = sink
        self.paths = []
        self.print_all_paths(source, sink)
        self.order = self.topological_order()
        self.placed = 0
        self.placement = None
        self.latency = None
        self.F = None
        self.RC = None
        self.selectivity = None

    # Function that finds the latency, F and RC of a DAG
    # given a placement of its nodes to devices and filter selectivity
    def calculate_objective_local(self, placement, selectivity):
        max_latency = -1
        for path in self.paths:
            path_latency = 0
            for node in path:

                dev1 = placement[node]
                # Execution latency
                path_latency += self.tasks[node].cpu_req / variables.cpu_capacity[dev1]

                # Transfer time from its parent node
                for parent in self.tasks[node].parents:
                    if parent in path:
                        dev2 = placement[parent]
                        path_latency += (self.tasks[parent].output_rate * selectivity * variables.com_cost[dev1][
                            dev2] / variables.bandwidth[dev1][dev2])

                # Transfer time from user if the node is a source
                if node == self.source:
                    path_latency += (
                            (self.tasks[node].input_rate * selectivity * variables.user_dev_cost[self.mobile_device_id][
                                dev1]) / variables.user_dev_bandwidth[self.mobile_device_id][dev1])

                # Transfer time to the user if the node is a sink
                if node == self.sink:
                    path_latency += (
                            (self.tasks[node].output_rate * selectivity *
                             variables.user_dev_cost[self.mobile_device_id][
                                 dev1]) / variables.user_dev_bandwidth[self.mobile_device_id][dev1])

            if path_latency > max_latency:  # Find the slowest path
                max_latency = path_latency

        enabled_cpu = (
            sum(variables.cpu_capacity[i] for i in Counter(placement)))  # Sum of enabled devices' CPU capacities
        enabled_sum = len(Counter(placement))  # Number of enabled devices
        filter_selectivity = selectivity

        RC = (enabled_sum ** (variables.RC_theta)) * (enabled_cpu / enabled_sum)
        F = (RC * max_latency) / ((variables.filter_alpha + filter_selectivity) ** variables.filter_beta)

        return round(max_latency, 3), round(F, 3), round(RC, 3)

    # Enforce a placement on the DAG
    def enforce_placement(self, placement, latency, F, RC, selectivity):
        self.placed = 1
        self.placement = placement
        self.latency = latency
        self.F = F
        self.RC = RC
        self.selectivity = selectivity

    # Remove the placment from the DAG
    def remove_placement(self):
        self.placed = 0
        self.placement = None
        self.latency = None
        self.F = None
        self.RC = None
        self.selectivity = None

    # A recursive function that finds all paths between two nodes
    def print_all_paths_util(self, u, d, visited, path):
        # Mark the current node as visited and store in path
        visited[u] = True
        path.append(u)
        # If current vertex is same as destination, then print
        # current path[]
        if u == d:
            self.paths.append(path[:])
        else:
            # If current vertex is not destination
            # Recur for all the vertices adjacent to this vertex
            for i in self.graph_dict[u]:
                if not visited[i]:
                    self.print_all_paths_util(i, d, visited, path)
                    # Remove current vertex from path[] and mark it as unvisited
        path.pop()
        visited[u] = False

    # Prints all paths from 's' to 'd'
    def print_all_paths(self, s, d):
        # Mark all the vertices as not visited
        visited = [False] * self.number_of_nodes
        # Create an array to store paths
        path = []
        # Call the recursive helper function to print all paths
        self.print_all_paths_util(s, d, visited, path)
        return self.paths

    # Function to find the topological order of a DAG
    def topological_order(self):
        order = []
        visited = [False] * self.number_of_nodes
        visited[self.source] = True
        for path in self.paths:  # For each path
            for i in path[::-1]:  # Reverse the nodes of each path
                if not visited[i]:
                    visited[i] = True
                    order.append(i)
        order.append(self.source)  # Add the source
        order = order[::-1]  # Reverse the order
        return order


# name of task: [CPU requirement, MEM requirement, selectivity]
type_of_task = {"Filter": [round(random.uniform(0.2, 1.1), 2), random.randint(1, 3), round(random.uniform(0.4, 1), 2)],
                "Scan": [round(random.uniform(0.2, 1.1), 2), random.randint(1, 3), round(random.uniform(0.4, 1), 2)],
                "Map": [round(random.uniform(0.2, 1.1), 2), random.randint(1, 3), round(random.uniform(0.4, 1), 2)],
                "GroupBy": [round(random.uniform(0.2, 1.1), 2), random.randint(1, 3), round(random.uniform(0.4, 1), 2)],
                "OrderBy": [round(random.uniform(0.2, 1.1), 2), random.randint(1, 3), round(random.uniform(0.4, 1), 2)]}


# Create a DAG in the form of a chain
def create_seq_dag(mobile_device_id, graph_id, number_of_nodes, user_rate):
    graph_dict = {}
    parents = {}
    tasks = []
    paths = []
    number_of_operators = number_of_nodes + 2
    source = 0
    sink = number_of_operators - 1
    output_rate = user_rate  # Rate of user
    for i in range(number_of_operators):
        parents[i] = []
        graph_dict[i] = []
        task_type, chars = random.choice(list(type_of_task.items()))  # Select a random type of task
        if i != sink:  # Non-sink nodes have children
            graph_dict[i].append(i + 1)
        if i != source:  # Non-source nodes have parents
            parents[i].append(i - 1)
        input_rate = output_rate
        # The output rate is calculated using the input rate and the task's selectivity
        output_rate = round(input_rate * chars[2], 2)
        # Create Task
        tasks.append(Task(i, task_type, chars[0], chars[1], chars[2], input_rate, output_rate, parents[i]))
    # Create DAG
    graph = Graph(mobile_device_id, graph_id, number_of_operators, graph_dict, source, sink, tasks, paths)
    return graph


# Create a DAG that has a single source and sink and 'n' parallel nodes between them
def create_diamond_dag(mobile_device_id, graph_id, number_of_nodes, user_rate):
    graph_dict = {}
    parents = {}
    tasks = []
    paths = []
    number_of_operators = number_of_nodes + 2
    source = 0
    sink = number_of_operators - 1
    output_rate = user_rate  # Rate of user

    for i in range(number_of_operators):
        parents[i] = []
        graph_dict[i] = []
    for i in range(number_of_operators):
        # Non-sink and non-source nodes have the source as their parent and the sink as their child
        if i != sink and i != source:
            graph_dict[i].append(sink)
            parents[i].append(source)
            graph_dict[source].append(i)
            parents[sink].append(i)
    for i in range(number_of_operators):
        if i == source:
            input_rate = output_rate
        else:
            input_rate = 0
            for parent in parents[i]:
                input_rate += tasks[parent].output_rate

        task_type, chars = random.choice(list(type_of_task.items()))  # Select a random type of task
        # The output rate is calculated using the input rate and the task's selectivity
        output_rate = round(input_rate * chars[2], 2)

        # Create task
        tasks.append(Task(i, task_type, chars[0], chars[1], chars[2], input_rate, output_rate, parents))
    # Create DAG
    graph = Graph(mobile_device_id, graph_id, number_of_operators, graph_dict, source, sink, tasks, paths)

    return graph


# Create a DAG that has a single source and sink and a line of '2n' parallel nodes and one of 'n' parallel nodes between them
def create_replicated_dag(mobile_device_id, graph_id, number_of_nodes, user_rate):
    graph_dict = {}
    parents = {}
    tasks = []
    paths = []
    number_of_operators = number_of_nodes + number_of_nodes * 2 + 2
    source = 0
    sink = number_of_operators - 1
    for i in range(number_of_operators):
        parents[i] = []
        graph_dict[i] = []
    # The first '2n' nodes (except the source) have the source as their parent
    for i in range(source + 1, number_of_nodes * 2 + 1):
        graph_dict[source].append(i)
        parents[i].append(source)

    # The first '2n' nodes (except the source) have the remaining ones (beside source and sink) as their children
    for i in range(source + 1, number_of_nodes * 2 + 1):
        for j in range(number_of_nodes * 2 + 1, number_of_nodes + number_of_nodes * 2 + 1):
            graph_dict[i].append(j)
            parents[j].append(i)

    # The remaining nodes (beside source and sink) have sink as their children
    for i in range(number_of_nodes * 2 + 1, number_of_nodes + number_of_nodes * 2 + 1):
        graph_dict[i].append(sink)
        parents[sink].append(i)

    output_rate = user_rate  # Rate of user
    for i in range(number_of_operators):
        task_type, chars = random.choice(list(type_of_task.items()))  # Select a random type of task
        if i != source:
            input_rate = 0
            for parent in parents[i]:
                input_rate += tasks[parent].output_rate
        else:
            input_rate = output_rate

        # The output rate is calculated using the input rate and the task's selectivity
        output_rate = round(input_rate * chars[2], 2)
        # Create task
        tasks.append(Task(i, task_type, chars[0], chars[1], chars[2], input_rate, output_rate, parents[i]))

    # Create graph
    graph = Graph(mobile_device_id, graph_id, number_of_operators, graph_dict, source, sink, tasks, paths)
    return graph
