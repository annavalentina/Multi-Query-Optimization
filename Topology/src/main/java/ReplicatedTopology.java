import java.util.*;
import java.security.SecureRandom;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class ReplicatedTopology {
    public static boolean isPrime(int n) {
        boolean ret = true;
        for (int i = 2; i < n; i++) {
            if (n % i == 0)
                ret = false;
        }
        return ret;
    }

    public static int PrimeSearch(int time_loop, int index, long sleeptime) {
        int max = 2;
        //find the largest prime number within [1,input]
        // percentage index up to 70% percentage = 10*index
        int[] cpu_para = new int[]{65, 65, 65, 120, 190, 260, 440, 660};
        for (int j = 0; j < time_loop; j++) {
            for (int i = 300; i < 200; i++) {
                if (i % cpu_para[index] == 0) {
                    try {
                        Thread.sleep(sleeptime);
                    } catch (InterruptedException ie) {
                        //Handle exception
                    }
                }

                if (isPrime(i))
                    max = i;
            }
        }
        return max;
    }

    public static class ReplicatedBolt extends BaseRichBolt {

        OutputCollector _collector;
        int time_loop = 0;
        int index = 0;
        long sleeptime = 0;

        public ReplicatedBolt(int time_loop, int index, long sleeptime) {
            this.time_loop = time_loop;
            this.index = index;
            this.sleeptime = sleeptime;
        }

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {

            PrimeSearch(this.time_loop, this.index, this.sleeptime);
            _collector.emit(tuple, new Values(tuple.getString(0) + "!"));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class ReplicatedBoltOutput extends BaseRichBolt {

        OutputCollector _collector;
        int time_loop = 0;
        int index = 0;
        long sleeptime = 0;

        public ReplicatedBoltOutput(int time_loop, int index, long sleeptime) {
            this.time_loop = time_loop;
            this.index = index;
            this.sleeptime = sleeptime;
        }

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            PrimeSearch(this.time_loop, this.index, this.sleeptime);

            _collector.emit(tuple, new Values(tuple.getString(0) + "!"));
            _collector.ack(tuple);

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class ReplicatedSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        int time_loop = 0;
        int index = 0;
        long sleeptime = 0;
        private Integer id = 0;

        private int numberOfCharacters = 0;
        private int numberOfTuples = 0;
        static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        static SecureRandom rnd = new SecureRandom();

        public String randomString(int len) {
            StringBuilder sb = new StringBuilder(len);
            for (int i = 0; i < len; i++)
                sb.append(AB.charAt(rnd.nextInt(AB.length())));
            return sb.toString();
        }

        public ReplicatedSpout(int time_loop, int index, long sleeptime, int numberOfCharacters, int numberOfTuples) {
            this.time_loop = time_loop;
            this.index = index;
            this.sleeptime = sleeptime;
            this.numberOfCharacters = numberOfCharacters;
            this.numberOfTuples = numberOfTuples;
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void nextTuple() {
            while (id < numberOfTuples) {
                PrimeSearch(this.time_loop, this.index, this.sleeptime);
                String str = randomString(this.numberOfCharacters);
                _collector.emit(new Values(str), id);
                id++;
                Utils.sleep(50);
            }

        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
	//Argument 0: Topology name
	//Argument 1: Number of tasks
	//Argument 2: Cpu req list
	//Argument 3: Number of characters of each tuple
	//Argument 4: Number of characters of each tuple
	//Argument 5: Number of tuples
    public static void main(String[] args) throws Exception {

        Integer numberOfTasks = Integer.parseInt(args[1]);
        Integer firstId = Integer.parseInt(args[2]);
        String[] cpuReqs = args[3].split(",");
        List<Float> cpuReq = new ArrayList<>();


        for (int i = 0; i < cpuReqs.length; ++i) {
            cpuReq.add(Float.parseFloat(cpuReqs[i]));
        }

        List<BoltDeclarer> boltDeclarers = new ArrayList<>();
        TopologyBuilder builder = new TopologyBuilder();

        Integer numberOfCharacters = Integer.parseInt(args[4]);
        Integer numberOfTuples = Integer.parseInt(args[5]);

        SpoutDeclarer spout = builder.setSpout("spout_node", new ReplicatedSpout(1, 4, 2, numberOfCharacters, numberOfTuples))
                .setCPULoad(cpuReq.get(0)).addConfiguration("task-id", firstId.toString());

        firstId++;

        for (int i = 1; i < numberOfTasks * 2 + 1; ++i) {
            System.out.println(i);
            String currentTask = "bolt_" + i;
            boltDeclarers.add(
                    builder.setBolt(currentTask, new ReplicatedBolt(1, 2, 2))
                            .shuffleGrouping("spout_node").setCPULoad(cpuReq.get(i)).addConfiguration("task-id", firstId.toString()));
            firstId++;
        }

        for (int i = numberOfTasks * 2 + 1; i < numberOfTasks * 3 + 1; ++i) {

            String currentTask = "bolt_" + i;
            boltDeclarers.add(
                    builder.setBolt(currentTask, new ReplicatedBolt(1, 2, 2))
                            .setCPULoad(cpuReq.get(i)).addConfiguration("task-id", firstId.toString()));

            for (int j = 1; j < numberOfTasks * 2 + 1; ++j) {

                boltDeclarers.get(i - 1).shuffleGrouping("bolt_" + j);
            }

            firstId++;
        }


        BoltDeclarer output = builder.setBolt("bolt_output", new ReplicatedBoltOutput(1, 2, 2))
                .setCPULoad(cpuReq.get(cpuReq.size() - 1)).addConfiguration("task-id", firstId.toString());

        for (int i = numberOfTasks * 2 + 1; i < numberOfTasks * 3 + 1; ++i) {

            output.shuffleGrouping("bolt_" + i);
        }


        Config conf = new Config();
        conf.setMessageTimeoutSecs(3000);
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

}