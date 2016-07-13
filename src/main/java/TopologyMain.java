/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */


import bolts.*;
import spouts.DrpcSpout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.starter.bolt.SingleJoinBolt;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Spout",new DrpcSpout());
        builder.setBolt("OfflineResultBolt", new GetOfflineResultBolt())
                .shuffleGrouping("Spout");
        builder.setBolt("UserHistoryBolt", new UserHistoryBolt())
                .shuffleGrouping("Spout");
        builder.setBolt("PushedArticalBolt", new PushedArticalBolt())
                .shuffleGrouping("Spout");
        builder.setBolt("JoinBolt", new SingleJoinBolt(new Fields("uin", "offlineresult","pushedartical","userhistory")),1)
                .fieldsGrouping("OfflineResultBolt", new Fields("uin"))
                .fieldsGrouping("UserHistoryBolt", new Fields("uin"))
                .fieldsGrouping("PushedArticalBolt", new Fields("uin"));
        builder.setBolt("FilterBoltOUP", new FilterBolt())
                .shuffleGrouping("JoinBolt");
        builder.setBolt("RegularMatchBolt", new RegularMatchBolt())
                .shuffleGrouping("FilterBoltOUP");
        builder.setBolt("FilterIdTopicBolt", new FilterIdTopicBolt())
                .shuffleGrouping("RegularMatchBolt");
        builder.setBolt("RankBolt", new RerankBolt())
                .shuffleGrouping("FilterIdTopicBolt");


        //Configuration
        Config conf = new Config();
        //conf.put("wordsFile", args[0]);
        conf.put("wordsFile", "C:\\Users\\Administrator\\IdeaProjects\\myheadline\\src\\main\\resources\\int.txt");
        conf.setDebug(false);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
        }catch(Exception ex){
            ex.printStackTrace();
        }
        Thread.sleep(1000);
        //cluster.shutdown();


    }
}
