package bolts;

import org.ansj.domain.Term;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.*;

/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */
public class FilterBolt extends BaseBasicBolt {
    double fencicanshu = 0.0;

    public void prepare(Map config, TopologyContext contex){
        this.fencicanshu = Double.parseDouble(config.get("fencicanshu").toString());
    }

    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {


        int uin = input.getIntegerByField("uin");
        List<OfflineResult> offlineresult = (LinkedList<OfflineResult>)input.getValueByField("offlineresult");
        List<PushedArtical> pushedartical = (LinkedList<PushedArtical>)input.getValueByField("pushedartical");
        List<UserHistory> userhistory = (LinkedList<UserHistory>)input.getValueByField("userhistory");


        //离线计算数据简单去重
        //System.out.println(offlineresult.size());
        Set<String> offlinetemp = new HashSet<String>();
        for(OfflineResult x:offlineresult){
            if(offlinetemp.contains(x.getid())){
                System.out.println("重复过滤！");
                offlineresult.remove(x);
            }else{
                offlinetemp.add(x.getid());
            }
        }
        //pushedartical去重-id
        //System.out.println(offlineresult.size());
        Set<String> pushedtemp = new HashSet<String>();
        for(PushedArtical x:pushedartical){
            pushedtemp.add(x.getid());
        }
        for(OfflineResult x:offlineresult){
            if(pushedtemp.contains(x.getid())){
                //System.out.println("pushedartical去重-id过滤！");
                x.setscore(0.0);
            }
        }
        //userhistory去重-id
        //System.out.println(offlineresult.size());
        Set<String> historytemp = new HashSet<String>();
        for(UserHistory x:userhistory){
            historytemp.add(x.getid());
        }
        for(OfflineResult x:offlineresult){
            if(historytemp.contains(x.getid())){
                //System.out.println("userhistory去重-id过滤！");
                x.setscore(0.0);
            }
        }
        //pushedartical去重-分词
        for(OfflineResult s:offlineresult){
            StringBuilder x1 = new StringBuilder();
            for(Term temp:ToAnalysis.parse(s.gettitle())){
                x1 = x1.append(" ").append(temp.getName());
            }
            //System.out.println(x1.toString());
            for(PushedArtical x:pushedartical){
                StringBuilder x2 = new StringBuilder();
                for(Term temp:ToAnalysis.parse(x.gettitle())){
                    x2 = x2.append(" ").append(temp.getName());
                }
                //System.out.println(x2.toString());
                Simi simi = new Simi();
                //System.out.println(simi.getSimilarity(x1.toString(),x2.toString()));
                if(simi.getSimilarity(x1.toString(),x2.toString())>=fencicanshu){
                    //System.out.println("pushedartical去重-分词过滤！");
                    s.setscore(0.0);
                    break;
                }
            }

        }

        //userhistory去重-分词
        for(OfflineResult s:offlineresult) {
            StringBuilder x1 = new StringBuilder();
            for (Term temp : ToAnalysis.parse(s.gettitle())) {
                x1 = x1.append(" ").append(temp.getName());
            }
            for (UserHistory x : userhistory) {
                StringBuilder x2 = new StringBuilder();
                for (Term temp : ToAnalysis.parse(x.gettitle())) {
                    x2 = x2.append(" ").append(temp.getName());
                }
                Simi simi = new Simi();
                if (simi.getSimilarity(x1.toString(), x2.toString()) >= fencicanshu) {
                    //System.out.println("userhistory去重-分词过滤！");
                    s.setscore(0.0);
                    break;
                }
            }
        }

        /*for(OfflineResult x:offlineresult){
            System.out.print(x.getid()+"     ");
            System.out.print(x.getscore()+"     ");
            System.out.print(x.gettitle()+"     ");
            System.out.print(x.gettopic()+"     ");
            System.out.println(x.geturl());
        }
        for(PushedArtical x:pushedartical){
            System.out.print(x.getid()+"     ");
            System.out.println(x.gettitle());
        }
        for(UserHistory x:userhistory){
            System.out.print(x.getid()+"     ");
            System.out.println(x.gettitle());
        }*/


        collector.emit(new Values(input.getValueByField("return-info"),uin,offlineresult));
    }



    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("return-info","uin+4","offlineresult"));
    }
}
