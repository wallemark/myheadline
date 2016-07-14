/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */


import bolts.*;
//import spouts.DrpcSpout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
//import org.apache.storm.LocalDRPC;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.starter.bolt.SingleJoinBolt;
import org.apache.storm.drpc.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();

        //LocalDRPC drpc = new LocalDRPC();
        //builder.setSpout("Spout", new DRPCSpout("uin", drpc));
        builder.setSpout("Spout", new DRPCSpout("uin"));


        builder.setBolt("OfflineResultBolt", new GetOfflineResultBolt())
                .shuffleGrouping("Spout");
        builder.setBolt("UserHistoryBolt", new UserHistoryBolt())
                .shuffleGrouping("Spout");
        builder.setBolt("PushedArticalBolt", new PushedArticalBolt())
                .shuffleGrouping("Spout");
        builder.setBolt("JoinBolt", new SingleJoinBolt(new Fields("return-info", "uin", "offlineresult","pushedartical","userhistory")),1)
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
        builder.setBolt("return", new ReturnResults())
                .shuffleGrouping("RankBolt");


        //Configuration
        Config conf = new Config();
        Properties p = new Properties();
        try {
            p.load(new FileInputStream("C:\\Users\\Administrator\\IdeaProjects\\myheadline\\src\\main\\resources\\Config.properties"));
            conf.put("url",p.getProperty("url"));
            conf.put("username",p.getProperty("username"));
            conf.put("password",p.getProperty("password"));
            conf.put("fencicanshu",p.getProperty("fencicanshu"));
            conf.put("gongzhonghaocanshu",p.getProperty("gongzhonghaocanshu"));
            conf.put("topiccanshu",p.getProperty("topiccanshu"));
            conf.put("biaotidangguolv1",p.getProperty("biaotidangguolv1"));
            conf.put("biaotidangguolv2",p.getProperty("biaotidangguolv2"));
            conf.put("biaotidangguolv3",p.getProperty("biaotidangguolv3"));
            conf.put("regular1",p.getProperty("regular1"));
            conf.put("regular2",p.getProperty("regular2"));
        } catch(IOException e) {
            e.printStackTrace();
        }
        conf.setDebug(false);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        if(args.length==0){
            /*LocalCluster cluster = new LocalCluster();
            try{
                cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
                System.out.println(drpc.execute("uin", "111111"));
                //Thread.sleep(2000);
                System.out.println(drpc.execute("uin", "222222"));
                //Thread.sleep(2000);
                System.out.println(drpc.execute("uin", "333333"));
                //Thread.sleep(2000);
                System.out.println(drpc.execute("uin", "444444"));
                //Thread.sleep(2000);
                System.out.println(drpc.execute("uin", "555555"));
                //Thread.sleep(2000);

            }catch(Exception ex){
                ex.printStackTrace();
            }
            Thread.sleep(1000);*/
        }else{
            try{
                StormSubmitter.submitTopology(args[0],conf,builder.createTopology());
            }catch(Exception e){
                e.printStackTrace();
            }
        }

    }
}
