import java.util.*;
import java.security.SecureRandom;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;

public class LinearTopology {

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

    public static class LinearBolt extends BaseRichBolt {

        OutputCollector _collector;
        int time_loop = 0;
        int index = 0;
        long sleeptime = 0;

        public LinearBolt(int time_loop, int index, long sleeptime) {
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

    public static class LinearBoltOutput extends BaseRichBolt {

        OutputCollector _collector;
        int time_loop = 0;
        int index = 0;
        long sleeptime = 0;

        public LinearBoltOutput(int time_loop, int index, long sleeptime) {
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

    public static class LinearSpout extends BaseRichSpout {
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

        public LinearSpout(int time_loop, int index, long sleeptime, int numberOfCharacters, int numberOfTuples) {
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
	//Argument 1: n (Total number of tasks = 1 source + n-2 tasks + 1 sink)
	//Argument 2: First id (if other topologies already exist)
	//Argument 3: CPU req list
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
        Integer numberOfCharacters = Integer.parseInt(args[4]);
        Integer numberOfTuples = Integer.parseInt(args[5]);

        List<BoltDeclarer> boltDeclarers = new ArrayList<>();
        TopologyBuilder builder = new TopologyBuilder();
        SpoutDeclarer spout = builder.setSpout("spout_node", new LinearSpout(1, 4, 2, numberOfCharacters, numberOfTuples))
                .setCPULoad(cpuReq.get(0)).addConfiguration("task-id", firstId.toString());

        firstId++;
        String previousTask = "spout_node";
        for (int i = 1; i < numberOfTasks - 1; ++i) {
            String currentTask = "bolt_" + i;
            boltDeclarers.add(
                    builder.setBolt(currentTask, new LinearBolt(1, 2, 2))
                            .shuffleGrouping(previousTask).setCPULoad(cpuReq.get(i)).addConfiguration("task-id", firstId.toString()));
            previousTask = currentTask;
            firstId++;
        }

        boltDeclarers.add(builder.setBolt("bolt_output", new LinearBoltOutput(1, 2, 2))
                .shuffleGrouping(previousTask).setCPULoad(cpuReq.get(cpuReq.size() - 1)).addConfiguration("task-id", firstId.toString()));


        Config conf = new Config();
        conf.setMessageTimeoutSecs(3000);
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}

