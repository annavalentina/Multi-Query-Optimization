# -------------------------------------------#
# Author: Michailidou Anna-Valentini
# Description:Algorithms that assign tasks
#   to devices.
# -------------------------------------------#

import copy
import gurobipy as gp
from gurobipy import GRB
import numpy as np
import variables
import itertools


# Calculate the optimal sampling ratio of a DAG, considering the trade-off with F
def find_sample_ratio(graph, placement):
    F_min = 10000
    final_selectivity = -1
    for selectivity in np.arange(0.1, 1.1, 0.1):
        max_latency, F, RC = graph.calculate_objective_local(placement, selectivity)
        if F < F_min:
            F_min = F
            final_selectivity = selectivity
    return final_selectivity


# Function that finds if the CPU and RAM constraints are fulfilled in the DP algorithm
def find_free_cpu_ram(graph, enabled_init, node, dev, input_devices):
    enabled = copy.deepcopy(enabled_init)
    enabled[1] -= graph.tasks[node].cpu_req
    enabled[2] -= graph.tasks[node].ram_req
    exam_dev = dev
    receives_from_dev = input_devices[node][dev]
    for node in range(node, 0, -1):  # For the previous nodes
        if receives_from_dev == exam_dev:
            enabled[1] -= graph.tasks[node - 1].cpu_req
            enabled[2] -= graph.tasks[node - 1].ram_req
        receives_from_dev = input_devices[node - 1][receives_from_dev]

    return 0 if enabled[1] < 0 or enabled[2] < 0 else 1


# Dynamic Programming algorithm to assign tasks to devices
def DP_placement(graph, enabled_init, availability, devices_to_remove):
    enabled = copy.deepcopy(enabled_init)
    min_cost = 10000
    final_placement = [-1] * graph.number_of_nodes
    costs = [[10000] * variables.number_of_edge_devices for i in range(graph.number_of_nodes)]
    input_devices = [[-1] * variables.number_of_edge_devices for i in range(graph.number_of_nodes)]

    # Execution time and transfer time from user to the source node
    for dev in range(variables.number_of_edge_devices):
        if graph.tasks[graph.source].cpu_req <= enabled[dev][1] and graph.tasks[graph.source].ram_req <= \
                enabled[dev][2] and availability[dev] == 1 and dev not in devices_to_remove:
            costs[graph.source][dev] = graph.tasks[graph.source].cpu_req / variables.cpu_capacity[dev] + (
                    (graph.tasks[graph.source].input_rate *
                     variables.user_dev_cost[graph.mobile_device_id][dev]) /
                    variables.user_dev_bandwidth[graph.mobile_device_id][dev])
        else:
            costs[graph.source][dev] = 10000

    # For the rest of the nodes
    for node in range(1, graph.number_of_nodes):

        for dev in range(variables.number_of_edge_devices):
            temp_cost_1 = 10000
            temp_dev = -1
            for dev2 in range(variables.number_of_edge_devices):  # Find from which device it is cheaper to receive data
                temp_cost_2 = costs[node - 1][dev2] + (
                        graph.tasks[node - 1].output_rate * variables.com_cost[dev][dev2] /
                        variables.bandwidth[dev][dev2])
                if temp_cost_2 < temp_cost_1:
                    temp_cost_1 = temp_cost_2
                    temp_dev = dev2
            costs[node][dev] = temp_cost_1 + (graph.tasks[node].cpu_req / variables.cpu_capacity[dev])
            input_devices[node][dev] = temp_dev
            # If CPU or RAM constraints are violated
            if (find_free_cpu_ram(graph, enabled[dev], node, dev, input_devices) == 0 or
                    availability[dev] == 0 or dev in devices_to_remove):
                costs[node][dev] = 10000
                input_devices[node][dev] = -1

    # Transfer time to user for the sink node
    for dev in range(variables.number_of_edge_devices):
        if (costs[graph.sink][dev]) != 10000:
            costs[graph.sink][dev] += (graph.tasks[graph.sink].output_rate *
                                       variables.user_dev_cost[graph.mobile_device_id][dev]) / \
                                      variables.user_dev_bandwidth[graph.mobile_device_id][dev]

    # Find the minimum cost of the last row
    for dev in range(variables.number_of_edge_devices):

        if min_cost > costs[graph.number_of_nodes - 1][dev]:
            min_cost = costs[graph.number_of_nodes - 1][dev]
            final_placement[graph.number_of_nodes - 1] = dev

    # Find the best placement in a bottom-up way
    for node in range(graph.number_of_nodes - 2, -1, -1):
        dev = final_placement[node + 1]
        final_placement[node] = input_devices[node + 1][dev]

    solution_found = 1

    for node in range(graph.number_of_nodes):
        if final_placement[node] == -1:
            solution_found = 0
            latency = F = RC = -1
            # break;
    if solution_found == 1:

        for node in range(graph.number_of_nodes):
            dev = final_placement[node]
            enabled[dev][0] = 1
            enabled[dev][1] = round(enabled[dev][1] - graph.tasks[node].cpu_req, 2)
            enabled[dev][2] = round(enabled[dev][2] - graph.tasks[node].ram_req, 2)
            enabled[dev][3] += 1
        latency, F, RC = graph.calculate_objective_local(final_placement, 1)
    return solution_found, latency, F, RC, final_placement, enabled


# Function that calls the DP algorithm based on the different optimization variations
def DP_placement_main(graph, enabled_init, resource_opt, devices_to_remove):
    enabled = copy.deepcopy(enabled_init)
    solution_found = 0
    placement = []
    availability = [1] * variables.number_of_edge_devices
    latency = F = RC = -1

    if resource_opt == "enabled":
        current_used = sum(
            enabled[i][0] for i in range(variables.number_of_edge_devices))  # Number of already enabled devices
        if current_used != 0:
            for dev in range(variables.number_of_edge_devices):
                if enabled[dev][0] == 0:  # If the device is not already enabled
                    availability[dev] = 0

    # Find initial DP solution
    solution_found_temp, latency_temp, F_temp, RC_temp, placement_temp, enabled_temp = \
        DP_placement(graph, enabled_init, availability, devices_to_remove)

    if solution_found_temp == 0 and resource_opt == "enabled":
        availability = [1] * variables.number_of_edge_devices
        solution_found_temp, latency_temp, F_temp, RC_temp, placement_temp, enabled_temp = \
            DP_placement(graph, enabled_init, availability, devices_to_remove)

    if solution_found_temp == 1:  # If a solution was found

        solution_found = solution_found_temp
        placement = copy.deepcopy(placement_temp)
        enabled = copy.deepcopy(enabled_temp)
        used_devices = set(placement)  # List of used devices
        number_of_used_devices = len(used_devices)  # Number of used devices
        latency = latency_temp
        F = F_temp
        RC = RC_temp

        if resource_opt == "min":
            for dev in range(variables.number_of_edge_devices):  # Set the non-used devices as unavailable
                if dev not in used_devices:
                    availability[dev] = 0
            # Run DP again and gradually remove devices
            for i in range(number_of_used_devices - 1):
                min_F = 100000
                for dev in used_devices:  # Remove a device from the list
                    availability[dev] = 0  # Set it as unavailable

                    # Run DP
                    solution_found_temp, latency_temp, F_temp, RC_temp, placement_temp, enabled_temp = \
                        DP_placement(graph, enabled_init, availability, devices_to_remove)
                    if solution_found_temp == 1 and F_temp < min_F:  # If a solution was found and is better than the previous one
                        device_to_remove = dev
                        min_F = F_temp
                    # If a solution was found and is better than the initial one
                    if solution_found_temp == 1 and F_temp < F:
                        solution_found = solution_found_temp
                        placement = copy.deepcopy(placement_temp)
                        enabled = copy.deepcopy(enabled_temp)
                        latency = latency_temp
                        F = F_temp
                        RC = RC_temp
                    availability[dev] = 1  # Set it as available again
                if min_F == 100000:  # If a solution was not found for any of the removed devices from the list
                    break
                # Else remove that device that was the most beneficial in terms of the objective function
                used_devices.remove(device_to_remove)
                availability[device_to_remove] = 0  # Set it as unavailable

    return solution_found, latency, F, RC, placement, enabled


# Quadratic Programming algorithm to assign tasks to devices
def QP_placement(graph, enabled_init, resource_opt, devices_to_remove):
    enabled = copy.deepcopy(enabled_init)
    latency = F = RC = 100000
    node_placement = []
    for node in range(graph.number_of_nodes):
        node_placement.append(-1)
    current_used = sum(
        enabled[i][0] for i in range(variables.number_of_edge_devices))  # Number of already enabled devices

    # print(resource_opt)
    start = 1 if current_used == 0 or resource_opt == "min" else 0
    if (resource_opt == "min"):
        end = variables.number_of_edge_devices
    else:
        end = variables.number_of_edge_devices - current_used + 1
    # for max_devices in range(start, end):
    #    print(max_devices)
    for max_devices in range(start, end):
        temp_placement = []
        for node in range(graph.number_of_nodes):
            temp_placement.append(-1)

        model = gp.Model("qp")
        model.Params.OutputFlag = 0
        latency = model.addVar(vtype=GRB.CONTINUOUS, lb=0, name="latency")  # The objective
        placement_list = list(range(variables.number_of_edge_devices))
        placement = dict()
        for i in range(graph.number_of_nodes):  # For each node
            placement[i] = model.addVars(placement_list, lb=0, ub=1, vtype=GRB.INTEGER, name="x")

        for node in range(graph.number_of_nodes):
            c1 = 0
            for dev in range(variables.number_of_edge_devices):
                c1 += placement[node][dev]
            model.addConstr(c1 == 1)  # Assign each node to exactly one device
            for dev in devices_to_remove:
                model.addConstr(placement[node][dev] == 0)
        if resource_opt != "lat":
            total_used = 0
            used_list = list(range(variables.number_of_edge_devices))
            used = model.addVars(used_list, lb=0, ub=1, vtype=GRB.INTEGER,
                                 name="used")  # 1 if the device takes on tasks, 0 otherwise
            for dev in range(variables.number_of_edge_devices):
                if enabled[dev][0] == 0 or resource_opt != "enabled":  # If the device is not already enabled
                    c1 = 0
                    for node in range(graph.number_of_nodes):
                        c1 += placement[node][dev]
                    # These constraints set 'used' equal to 1 if the device takes on tasks or equal to 0 if it does not
                    model.addConstr(used[dev] >= c1 / 100)
                    model.addConstr(used[dev] <= c1 / 100 + 0.99)
                total_used += used[dev]
            model.addConstr(total_used <= max_devices)  # Bound the maximum amount of devices that will be used

        # Set CPU, RAM constraints
        for dev in range(variables.number_of_edge_devices):
            c2 = 0
            c3 = 0
            for node in range(graph.number_of_nodes):
                c2 += placement[node][dev] * graph.tasks[node].cpu_req
                c3 += placement[node][dev] * graph.tasks[node].ram_req
            model.addConstr(c2 <= enabled[dev][1])
            model.addConstr(c3 <= enabled[dev][2])

        # For each path
        for path in graph.paths:
            path_latency = 0
            for node in path:
                c = 0
                # Execution time
                for dev in range(variables.number_of_edge_devices):
                    c += placement[node][dev] * graph.tasks[node].cpu_req / variables.cpu_capacity[dev]
                path_latency += c

                # Transfer time from its parent node
                for parent in graph.tasks[node].parents:
                    if parent in path:
                        c = 0
                        for dev1, dev2 in list(itertools.product(range(variables.number_of_edge_devices),
                                                                 range(variables.number_of_edge_devices))):
                            c += ((graph.tasks[parent].output_rate * variables.com_cost[dev1][dev2] *
                                   placement[node][dev1] * placement[parent][dev2]) / variables.bandwidth[dev1][dev2])
                        path_latency += c

                # Transfer time from user if the node is a source
                if node == graph.source:
                    c = 0
                    for dev in range(variables.number_of_edge_devices):
                        c += ((graph.tasks[node].input_rate * placement[node][dev] *
                               variables.user_dev_cost[graph.mobile_device_id][dev]) /
                              variables.user_dev_bandwidth[graph.mobile_device_id][dev])
                    path_latency += c

                # Transfer time to the user if the node is a sink
                if node == graph.sink:
                    c = 0
                    for dev in range(variables.number_of_edge_devices):
                        c += ((graph.tasks[node].output_rate * placement[node][dev] *
                               variables.user_dev_cost[graph.mobile_device_id][dev]) /
                              variables.user_dev_bandwidth[graph.mobile_device_id][dev])
                    path_latency += c

            model.addConstr(latency >= path_latency)  # Minimize the slowest path of the DAG

        model.setObjective(latency, GRB.MINIMIZE)
        model.optimize()  # Solve QP
        if model.STATUS != GRB.OPTIMAL:  # If a solution was not found
            solved = -1
            if resource_opt == "lat":
                break
        else:  # If a solution was found
            solved = 1
            # model.write("solution.sol")
            for node in range(graph.number_of_nodes):
                for dev in range(variables.number_of_edge_devices):
                    if round(placement[node][dev].x) == 1:
                        temp_placement[node] = dev
                        enabled[dev][0] = 1
                        enabled[dev][1] = round(enabled[dev][1] - graph.tasks[node].cpu_req, 2)
                        enabled[dev][2] = round(enabled[dev][2] - graph.tasks[node].ram_req, 2)
                        enabled[dev][3] += 1
            latency_temp, F_temp, RC_temp = graph.calculate_objective_local(temp_placement, 1)

            F = F_temp
            latency = latency_temp
            RC = RC_temp
            node_placement = copy.deepcopy(temp_placement)
            break
    return solved, latency, F, RC, node_placement, enabled
