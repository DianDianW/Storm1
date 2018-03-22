import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyWordCount {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        //设置数据源
        builder.setSpout("spout", new CreateSpout(), 1);
        //读取spout的数据源,完成切分字符串的操作
        builder.setBolt("split", new SplitBolt(), 1).shuffleGrouping("spout");
        //读取split后的数据,进行count(tick周期10秒)
        builder.setBolt("count", new CountBolt(), 1).fieldsGrouping("split", new Fields("word"));
        //读取show之后的缓冲后的数据,进行最终的打印
        builder.setBolt("final", new PrintBolt(), 1).shuffleGrouping("count");
        /*---------------套路--------------------*/
        Config config = new Config();
        config.setDebug(false);
        //集群模式
        if (args != null && args.length > 0) {
            config.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            //单机模式
        } else {
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", config, builder.createTopology());
            Thread.sleep(3000000);
            cluster.shutdown();
        }
    }
}