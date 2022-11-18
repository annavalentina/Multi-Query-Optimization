# -------------------------------------------#
# Author: Michailidou Anna-Valentini
# Description:This file is used to configure
# the experiment variables and define the
# fixed variables' values
# -------------------------------------------#

import random
import DAG
import itertools
import copy


def init():
    global number_of_edge_devices
    global number_of_user_devices
    global number_of_dags_per_user
    global cpu_capacity
    global ram_capacity
    global com_cost
    global user_dev_cost
    global bandwidth
    global user_dev_bandwidth
    global RC_theta
    global filter_alpha
    global filter_beta
    global filter_selectivities

    # Fixed variables
    RC_theta = 1.5
    filter_alpha = 0.5
    filter_beta = 0.5
    filter_selectivities = []

    # Number of dags, users, devices
    number_of_dags_per_user = 10
    number_of_user_devices = 2
    number_of_edge_devices = 50

    # Set cpu and ram capacities for the devices
    cpu_capacity = []
    ram_capacity = []

    for i in range(number_of_edge_devices):
        cpu = round(random.uniform(2, 10))
        ram = random.randint(4, 32)
        cpu_capacity.append(cpu)
        ram_capacity.append(ram)

    # Set communication cost and bandwidth between pairs of devices
    com_cost = [[0 for i in range(number_of_edge_devices)] for j in
                range(number_of_edge_devices)]  # Communication cost for each pair of devices
    bandwidth = [[1 for i in range(number_of_edge_devices)] for j in
                 range(number_of_edge_devices)]  # Bandwidth for each pair of devices

    for dev1, dev2 in list(itertools.product(range(number_of_edge_devices), range(number_of_edge_devices))):
        if dev1 < dev2 and dev1 != dev2:
            cost = round(random.uniform(0.1, 10), 2)
            com_cost[dev1][dev2] = cost
            com_cost[dev2][dev1] = cost

            bdw = round(random.uniform(10, 100), 2)
            bandwidth[dev1][dev2] = bdw
            bandwidth[dev2][dev1] = bdw

    # Set communication cost and bandwidth between pairs of users and devices
    user_dev_cost = [[0 for i in range(number_of_edge_devices)] for j in
                     range(number_of_user_devices)]  # Communication cost for each pair of device and user
    user_dev_bandwidth = [[0 for i in range(number_of_edge_devices)] for j in
                          range(number_of_user_devices)]  # Bandwidth for each pair of device and user
    for user, dev in list(itertools.product(range(number_of_user_devices), range(number_of_edge_devices))):
        cost = round(random.uniform(0.1, 10), 2)
        user_dev_cost[user][dev] = cost

        bdw = round(random.uniform(10, 100), 2)
        user_dev_bandwidth[user][dev] = bdw


# Initialize the graphs
def create_graphs():
    graphs = []
    type_of_dag = {"S", "D", "R"}
    for i in range(number_of_user_devices):  # For each user
        for j in range(number_of_dags_per_user):
            input_rate = random.randint(1, 10)
            number_of_tasks = random.randint(1, 3)
            dag_type = random.choice(list(type_of_dag))
            if dag_type == "S":
                graphs.append(DAG.create_seq_dag(i, j + (i * number_of_dags_per_user), number_of_tasks, input_rate))
            elif dag_type == "D":
                graphs.append(DAG.create_diamond_dag(i, j + (i * number_of_dags_per_user), number_of_tasks, input_rate))
            else:
                graphs.append(
                    DAG.create_replicated_dag(i, j + (i * number_of_dags_per_user), number_of_tasks, input_rate))
    return graphs


# This class represents the setting of an experiment instance
class AlgSetting:
    def __init__(self, graphs, enabled):
        self.graphs = graphs
        self.enabled = enabled
        self.sum_latency = 0
        self.sum_filter_ratios = 0
        self.number_of_placed_dags = 0

    def calculate_enabled(self):
        self.sum_latency = 0
        self.sum_filter_ratios = 0
        self.number_of_placed_dags = 0
        self.enabled.clear()
        for dev in range(number_of_edge_devices):
            self.enabled.append([0, 0, 0, 0])  # Values:[enabled, free cpu capacity, free ram capacity, number of tasks]
            self.enabled[dev][1] = cpu_capacity[dev]
            self.enabled[dev][2] = ram_capacity[dev]
        for graph in self.graphs:
            if graph.placed:
                for node in range(graph.number_of_nodes):
                    dev_temp = graph.placement[node]
                    self.enabled[dev_temp][0] = 1
                    self.enabled[dev_temp][1] = round(self.enabled[dev_temp][1] - graph.tasks[node].cpu_req, 2)
                    self.enabled[dev_temp][2] = round(self.enabled[dev_temp][2] - graph.tasks[node].ram_req, 2)
                    self.enabled[dev_temp][3] += 1
                self.sum_latency += graph.latency
                self.sum_filter_ratios += graph.selectivity
                self.number_of_placed_dags += 1


# Create an experiment instance
def set_alg_setting(graphs_init):
    graphs = copy.deepcopy(graphs_init)
    enabled = []

    for dev in range(number_of_edge_devices):
        enabled.append([0, 0, 0, 0])  # Values:[enabled, free cpu capacity, free ram capacity, number of tasks]
        enabled[dev][1] = cpu_capacity[dev]
        enabled[dev][2] = ram_capacity[dev]
    return AlgSetting(graphs, enabled)
