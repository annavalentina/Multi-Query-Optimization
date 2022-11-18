# -------------------------------------------#
# Author: Michailidou Anna-Valentini
# Description: Main class that calls the
# different optimization algorithms
# -------------------------------------------#

from PlacementFunctions import *
import variables


# Function that finds the F objective of all the DAGs
def calculate_objective_global(setting):
    if setting.number_of_placed_dags == 0:
        return -1
    else:
        enabled_cpu = (sum(variables.cpu_capacity[i] for i in range(variables.number_of_edge_devices) if
                           setting.enabled[i][0] != 0))  # Sum of enabled devices' CPU capacities
        enabled_sum = (
            sum(setting.enabled[i][0] for i in range(variables.number_of_edge_devices)))  # Number of enabled devices
        filter_selectivity_avg = setting.sum_filter_ratios / setting.number_of_placed_dags

        RC = (enabled_sum ** (variables.RC_theta)) * (enabled_cpu / enabled_sum)
        F = (RC * setting.sum_latency) / ((variables.filter_alpha + filter_selectivity_avg) ** variables.filter_beta)
        return round(F, 3), round(RC, 3), round(setting.sum_latency, 3), round(filter_selectivity_avg, 3)


# Optimize F based on the objectives
def optimize_global_F_by_objective(algorithm, opt_type, setting, F_global, objective, gamma):
    setting_temp = copy.deepcopy(setting)
    x = 4
    dags = []
    devices = []
    for graph in setting_temp.graphs:
        if graph.placed == 1:
            if objective == "F":
                dags.append([graph.graph_id, graph.F])
            elif objective == "RC":
                dags.append([graph.graph_id, graph.RC])
            else:
                dags.append([graph.graph_id, graph.latency])

    # Find the top-x dags with the higher objective
    dags = sorted(dags, key=lambda l: l[1], reverse=True)[:x]
    for dag in dags:
        graph = setting_temp.graphs[dag[0]]
        for dev in graph.placement:
            if dev not in devices:
                devices.append(dev)

    # Calculate the average cpu and ram of the devices utilized for the top-x dags
    avg_cpu = 0
    for dev in devices:
        avg_cpu += variables.cpu_capacity[dev]
    avg_cpu = avg_cpu / len(devices)

    for dag in dags:
        graph = setting_temp.graphs[dag[0]]
        graph.remove_placement()
    setting_temp.calculate_enabled()

    # Mark the devices whose cpu capacity is lower than the average
    devices_not_to_use = []
    for dev in range(variables.number_of_edge_devices):
        if setting_temp.enabled[dev][0] == 0 and variables.cpu_capacity[dev] < avg_cpu * gamma:
            devices_not_to_use.append(dev)

    # Find a placement without using these devices
    for dag in dags:
        graph = setting_temp.graphs[dag[0]]
        run_algorithm(algorithm, opt_type, graph, setting_temp, devices_not_to_use)
        if not graph.placed:
            run_algorithm(algorithm, opt_type, graph, setting_temp, [])
    F_global_new, RC_global_new, latency_global_new = calculate_objective_global(setting_temp)

    # If a better solution was found keep it, otherwise discard it
    if F_global_new > F_global or setting_temp.number_of_placed_dags != setting.number_of_placed_dags:
        return setting
    for dev in setting_temp.enabled:
        if (dev[1] < 0 or dev[2] < 0):
            print("error")
    return setting_temp


# Optimize F based on the resource utilization
def optimize_global_F_byUtil(algorithm, opt_type, setting, F_global, delta):
    # Find the less utilized devices based on a threshold
    setting_temp = copy.deepcopy(setting)
    devices_not_to_use = []
    for dev in range(variables.number_of_edge_devices):
        if setting_temp.enabled[dev][0] == 1 and (variables.cpu_capacity[dev] - setting_temp.enabled[dev][1]) / \
                variables.cpu_capacity[dev] < delta:
            devices_not_to_use.append(dev)

    # Find the dags that use these devices
    dags = []
    for dev in devices_not_to_use:
        for graph in setting_temp.graphs:
            if graph.placed == 1 and dev in graph.placement:
                graph.remove_placement()
                dags.append(graph.graph_id)
                setting_temp.calculate_enabled()

    # Find a placement without using these devices
    for dag in dags:
        graph = setting_temp.graphs[dag]
        run_algorithm(algorithm, opt_type, graph, setting_temp, devices_not_to_use)
        if not graph.placed:
            run_algorithm(algorithm, opt_type, graph, setting_temp, [])
    F_global_new, RC_global_new, latency_global_new = calculate_objective_global(setting_temp)

    # If a better solution was found keep it, otherwise discard it
    if F_global_new > F_global or setting_temp.number_of_placed_dags != setting.number_of_placed_dags:
        return setting
    for dev in setting_temp.enabled:
        if (dev[1] < 0 or dev[2] < 0):
            print("error")
    return setting_temp


# Function that runs the optimization algorithms
def run_algorithm(algorithm, opt_type, graph, setting, devices_to_remove):
    f_lat = open("latency.csv", "a")
    f_F = open("F.csv", "a")
    f_RC = open("RC.csv", "a")
    f_sel = open("selectivity.csv", "a")
    if algorithm == "QP":  # Quadratic Programming
        solved, latency, F, RC, placement, enabled_sol = QP_placement(graph, setting.enabled, opt_type,
                                                                      devices_to_remove)
    else:  # Dynamic Programming
        solved, latency, F, RC, placement, enabled_sol = DP_placement_main(graph, setting.enabled, opt_type,
                                                                           devices_to_remove)
    if solved == 1:
        # Find the optimal sampling ratio
        setting.enabled = copy.deepcopy(enabled_sol)
        selectivity = find_sample_ratio(graph, placement)

        # Calculate latency, F, RC objectives
        latency, F, RC = graph.calculate_objective_local(placement, selectivity)

        # Enforce placement on graph
        graph.enforce_placement(placement, latency, F, RC, selectivity)
        setting.calculate_enabled()
        f_lat.write(str(latency) + " ")
        f_F.write(str(F) + " ")
        f_RC.write(str(RC) + " ")
        f_sel.write(str(selectivity) + " ")
    else:
        f_lat.write("-1 ")
        f_F.write("-1 ")
        f_RC.write("-1 ")
        f_sel.write("-1 ")
    f_lat.close()
    f_RC.close()
    f_F.close()
    f_sel.close()


def main_experiment():
    f_lat = open("latency.csv", "a")
    f_F = open("F.csv", "a")
    f_RC = open("RC.csv", "a")
    f_F_global = open("F_global.csv", "a")
    f_RC_global = open("RC_global.csv", "a")
    f_latency_global = open("latency_global.csv", "a")
    f_sel = open("selectivity.csv", "a")
    f_sel_global = open("selectivity_global.csv", "a")

    f_lat.write("QP_lat QP_enabled QP_min DP_lat DP_enabled DP_min  \n")
    f_RC.write("QP_lat QP_enabled QP_min DP_lat DP_enabled DP_min  \n")
    f_F.write("QP_lat QP_enabled QP_min DP_lat DP_enabled DP_min  \n")
    f_F_global.write("QP_lat QP_enabled QP_min DP_lat DP_enabled DP_min  \n")
    f_RC_global.write("QP_lat QP_enabled QP_min DP_lat DP_enabled DP_min  \n")
    f_latency_global.write("QP_lat QP_enabled QP_min DP_lat DP_enabled DP_min  \n")
    f_sel.write("QP_lat QP_enabled QP_min DP_lat DP_enabled DP_min  \n")
    f_sel_global.write("QP_lat QP_enabled QP_min DP_lat DP_enabled DP_min  \n")

    f_F_global.close()
    f_RC_global.close()
    f_latency_global.close()
    f_F.close()
    f_RC.close()
    f_sel.close()
    f_sel_global.close()

    for iter in range(50):
        variables.init()
        graphs_init = variables.create_graphs()
        QP_lat_setting = variables.set_alg_setting(graphs_init)
        QP_enabled_setting = variables.set_alg_setting(graphs_init)
        QP_min_setting = variables.set_alg_setting(graphs_init)
        DP_lat_setting = variables.set_alg_setting(graphs_init)
        DP_enabled_setting = variables.set_alg_setting(graphs_init)
        DP_min_setting = variables.set_alg_setting(graphs_init)

        for i in range(len(graphs_init)):
            f_lat = open("latency.csv", "a")
            f_F = open("F.csv", "a")
            f_RC = open("RC.csv", "a")
            f_sel = open("selectivity.csv", "a")

            f_F_global = open("F_global.csv", "a")
            f_RC_global = open("RC_global.csv", "a")
            f_latency_global = open("latency_global.csv", "a")
            f_sel_global = open("selectivity_global.csv", "a")
            devices_to_remove = []

            run_algorithm("QP", "lat", QP_lat_setting.graphs[i], QP_lat_setting, devices_to_remove)
            run_algorithm("QP", "enabled", QP_enabled_setting.graphs[i], QP_enabled_setting, devices_to_remove)
            run_algorithm("QP", "min", QP_min_setting.graphs[i], QP_min_setting, devices_to_remove)
            run_algorithm("DP", "lat", DP_lat_setting.graphs[i], DP_lat_setting, devices_to_remove)
            run_algorithm("DP", "enabled", DP_enabled_setting.graphs[i], DP_enabled_setting, devices_to_remove)
            run_algorithm("DP", "min", DP_min_setting.graphs[i], DP_min_setting, devices_to_remove)

            F_QP_lat, RC_QP_lat, latency_QP_lat, sel_QP_lat = calculate_objective_global(QP_lat_setting)
            F_QP_enabled, RC_QP_enabled, latency_QP_enabled, sel_QP_enabled = calculate_objective_global(
                QP_enabled_setting)
            F_QP_min, RC_QP_min, latency_QP_min, sel_QP_min = calculate_objective_global(QP_min_setting)
            F_DP_lat, RC_DP_lat, latency_DP_lat, sel_DP_lat = calculate_objective_global(DP_lat_setting)
            F_DP_enabled, RC_DP_enabled, latency_DP_enabled, sel_DP_enabled = calculate_objective_global(
                DP_enabled_setting)
            F_DP_min, RC_DP_min, latency_DP_min, sel_DP_min = calculate_objective_global(DP_min_setting)

            f_F_global.write(str(F_QP_lat) + " " + str(F_QP_enabled) + " " + str(F_QP_min) + " " + str(F_DP_lat)
                             + " " + str(F_DP_enabled) + " " + str(F_DP_min) + " \n")
            f_RC_global.write(str(RC_QP_lat) + " " + str(RC_QP_enabled) + " " + str(RC_QP_min) + " " + str(RC_DP_lat)
                              + " " + str(RC_DP_enabled) + " " + str(RC_DP_min) + " \n")
            f_latency_global.write(
                str(latency_QP_lat) + " " + str(latency_QP_enabled) + " " + str(latency_QP_min) + " " + str(
                    latency_DP_lat)
                + " " + str(latency_DP_enabled) + " " + str(latency_DP_min) + " \n")
            f_sel_global.write(
                str(sel_QP_lat) + " " + str(sel_QP_enabled) + " " + str(sel_QP_min) + " " + str(
                    sel_DP_lat) + " " + str(sel_DP_enabled) + " " + str(sel_DP_min) + " \n")

            f_lat.write("\n")
            f_F.write("\n")
            f_RC.write("\n")
            f_sel.write("\n")

        f_F_global.close()
        f_RC_global.close()
        f_latency_global.close()
        f_sel_global.close()


main_experiment()
