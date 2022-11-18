package com.schedulers;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class CustomScheduler implements IScheduler {
    public static void main(String[] args) {
    }

    @Override
    public void prepare(Map<String, Object> map, StormMetricsRegistry stormMetricsRegistry) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        Collection<TopologyDetails> topologyDetails = topologies.getTopologies();//Get all topologies
        Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();//Get all supervisors
        //Create HashMap <task id, supervisor id>
        Map<Integer, Integer> taskToSupervisor = new HashMap<Integer, Integer>();
        try {
            File myObj = new File("/jars/config.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String[] ids = myReader.nextLine().split("\\s+");
                taskToSupervisor.put(Integer.parseInt(ids[0]), Integer.parseInt(ids[1]));
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }


        //Create HashMap <supervisor id, supervisor details>
        Map<Integer, SupervisorDetails> supervisors = new HashMap<Integer, SupervisorDetails>();
        for (SupervisorDetails s : supervisorDetails) {
            Map<String, Object> metadata = (Map<String, Object>) s.getSchedulerMeta();
            if (metadata.get("group-id") != null) {
                supervisors.put(Integer.parseInt((String) metadata.get("group-id")), s);
            }
        }


        for (TopologyDetails t : topologyDetails) {
            HashMap<SupervisorDetails, List<ExecutorDetails>> executorsToSupervisors = new HashMap<SupervisorDetails, List<ExecutorDetails>>();
            //if (!cluster.needsScheduling(t)) continue;
            //If the topology needs scheduling
            StormTopology topology = t.getTopology();
            Map<String, SpoutSpec> spouts = topology.get_spouts();
            Map<String, Bolt> bolts = topology.get_bolts();


            JSONParser parser = new JSONParser();
            Integer spout_id=0;
            try {
                for (String name : spouts.keySet()) {//For each spout
                    //Assign them based on the tags
                    SpoutSpec spout = spouts.get(name);
                    JSONObject conf = (JSONObject) parser.parse(spout.get_common().get_json_conf());
                    Integer tid = Integer.parseInt((String) conf.get("task-id"));//Get task id of the spout
                    spout_id=tid;
                    Integer gid = taskToSupervisor.get(tid);//Find group id of the spout
                    if (conf.get("task-id") != null && supervisors.get(gid) != null) {
                        SupervisorDetails supervisor = supervisors.get(gid);//Get the correct supervisor
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);//Get slots of the supervisor
                        List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);//Get executors of the spout
                        if (!availableSlots.isEmpty() && executors != null) {
                            if (executorsToSupervisors.containsKey(supervisor)) {//If the HasMap already contains this supervisor add the executor to its list
                                executorsToSupervisors.get(supervisor).addAll(executors);
                            } else {
                                executorsToSupervisors.put(supervisor, executors);//Otherwise create entry for this supervisor
                            }
                        }
                    }
                }
                for (String name : bolts.keySet()) {//For each bolt
                    //Assign them based on the tags
                    Bolt bolt = bolts.get(name);
                    JSONObject conf = (JSONObject) parser.parse(bolt.get_common().get_json_conf());
                    Integer tidB = Integer.parseInt((String) conf.get("task-id"));//Get task id of the bolt
                    Integer gidB = taskToSupervisor.get(tidB);//Find group id of the bolt
                    if (conf.get("task-id") != null && supervisors.get(gidB) != null) {
                        SupervisorDetails supervisor = supervisors.get(gidB);//Get the correct supervisor
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);//Get slots of the supervisor
                        List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);//Get executors of the spout
                        if (!availableSlots.isEmpty() && executors != null) {
                            if (executorsToSupervisors.containsKey(supervisor)) {//If the HasMap already contains this supervisor add the executor to its list
                                executorsToSupervisors.get(supervisor).addAll(executors);
                            } else {
                                executorsToSupervisors.put(supervisor, executors);//Otherwise create entry for this supervisor
                            }
                        }
                    }
                }
                for (String c : cluster.getNeedsSchedulingComponentToExecutors(t).keySet()) {
                    if (c.startsWith("__")) {//Ack tasks etc
                        if (supervisors.get(1) != null) {
                            SupervisorDetails supervisor = supervisors.get(1);//Get the first supervisor
                            List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
                            List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(c);
                            if (!availableSlots.isEmpty()) {
                                if (executorsToSupervisors.containsKey(supervisor)) {//If the HasMap already contains this supervisor add the executor to its list
                                    executorsToSupervisors.get(supervisor).addAll(executors);
                                } else {
                                    executorsToSupervisors.put(supervisor, executors);//Otherwise create entry for this supervisor
                                }
                            }
                        }
                    }
                }
                for (Map.Entry<SupervisorDetails, List<ExecutorDetails>> entry : executorsToSupervisors.entrySet()) {//Actual assignment to supervisors
                    SupervisorDetails supervisor = entry.getKey();
                    List<ExecutorDetails> executors = entry.getValue();
                    List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
                    List<ExecutorDetails> execToAssign = new ArrayList<>();
                    if (!executors.isEmpty() && !availableSlots.isEmpty()) {
                        cluster.assign(availableSlots.get(0), t.getId(), executors);
                    }
                }
            } catch (ParseException pe) {
                pe.printStackTrace();
            }
        }
    }

    @Override
    public Map config() {
        return new HashMap();
    }
}
