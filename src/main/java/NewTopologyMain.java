/**
 * Created by Administrator on 2016/7/6.
 */
//ryanyycao edit;

import spouts.DrpcSpout;
import bolts.GetOfflineResultBolt;
import bolts.UserHistoryBolt;
import bolts.PushedArticalBolt;
import bolts.Filter1Bolt;
import bolts.RerankBolt;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.starter.bolt.SingleJoinBolt;

public class NewTopologyMain {
    public static void main(String[] args) throws InterruptedException {
        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("uni",new DrpcSpout());
        builder.setBolt("uni+1", new GetOfflineResultBolt())
                .shuffleGrouping("uni");
        builder.setBolt("uni+2", new UserHistoryBolt())
                .shuffleGrouping("uni");
        builder.setBolt("uni+3", new PushedArticalBolt())
                .shuffleGrouping("uni");
        builder.setBolt("JoinBolt", new SingleJoinBolt(new Fields("uin", "offlineresult","pushedartical","userhistory")),1)
                .fieldsGrouping("uni+1", new Fields("uni"))
                .fieldsGrouping("uni+2", new Fields("uni"))
                .fieldsGrouping("uni+3", new Fields("uni"));
        builder.setBolt("uni+4", new Filter1Bolt())
                .shuffleGrouping("JoinBolt");
        /*builder.setBolt("uni+5", new RerankBolt())
                .shuffleGrouping("uni+4");*/


        //Configuration
        Config conf = new Config();
        //conf.put("wordsFile", args[0]);
        conf.put("wordsFile", "F:\\storm-book-examples-ch02-getting_started-8e42636\\src\\main\\resources\\int.txt");
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
