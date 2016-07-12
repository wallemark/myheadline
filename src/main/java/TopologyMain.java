/**
 * Created by Administrator on 2016/7/6.
 */
//ryanyycao edit;

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
        builder.setSpout("uin",new DrpcSpout());
        builder.setBolt("uin+1", new GetOfflineResultBolt())
                .shuffleGrouping("uin");
        builder.setBolt("uin+2", new UserHistoryBolt())
                .shuffleGrouping("uin");
        builder.setBolt("uin+3", new PushedArticalBolt())
                .shuffleGrouping("uin");
        builder.setBolt("JoinBolt", new SingleJoinBolt(new Fields("uin", "offlineresult","pushedartical","userhistory")),1)
                .fieldsGrouping("uin+1", new Fields("uin"))
                .fieldsGrouping("uin+2", new Fields("uin"))
                .fieldsGrouping("uin+3", new Fields("uin"));
        builder.setBolt("Join_filter", new FilterBolt())
                .shuffleGrouping("JoinBolt");
        builder.setBolt("RegularMatch", new RegularMatchBolt())
                .shuffleGrouping("Join_filter");
        builder.setBolt("Filter_id_topic", new FilterIdTopicBolt())
                .shuffleGrouping("RegularMatch");
        builder.setBolt("result", new RerankBolt())
                .shuffleGrouping("Filter_id_topic");


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
