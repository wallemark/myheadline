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
    Simi simi;

    public void prepare(Map config, TopologyContext contex){
        this.fencicanshu = Double.parseDouble(config.get("fencicanshu").toString());
        this.simi = new Simi();
        for (Term temp : ToAnalysis.parse("hello world")) {
            String chushi = temp.getName();
        }
    }

    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {


        long uin = input.getLongByField("uin");
        List<OfflineResult> offlineresult = (LinkedList<OfflineResult>)input.getValueByField("offlineresult");
        List<PushedArtical> pushedartical = (LinkedList<PushedArtical>)input.getValueByField("pushedartical");
        List<UserHistory> userhistory = (LinkedList<UserHistory>)input.getValueByField("userhistory");


        System.out.println(System.currentTimeMillis());
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
                System.out.println("pushedartical去重-id过滤！");
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
                System.out.println("userhistory去重-id过滤！");
                x.setscore(0.0);
            }
        }
        //pushedartical去重-分词
        /*for(OfflineResult s:offlineresult){
            StringBuilder x1 = new StringBuilder();
            for(Term temp:ToAnalysis.parse(s.gettitle())){
                x1 = x1.append(" ").append(temp.getName());
            }
            for(PushedArtical x:pushedartical){
                StringBuilder x2 = new StringBuilder();
                for(Term temp:ToAnalysis.parse(x.gettitle())){
                    x2 = x2.append(" ").append(temp.getName());
                }
                Simi simi = new Simi();
                if(simi.getSimilarity(x1.toString(),x2.toString())>=fencicanshu){
                    s.setscore(0.0);
                    break;
                }
            }
        }*/
        //分词；

        List<Set<String>> list1 = new LinkedList<Set<String>>();
        for(OfflineResult x:offlineresult){
            Set<String> listtt = new HashSet<String>();
            for (Term temp : ToAnalysis.parse(x.gettitle())) {
                listtt.add(temp.getName());
            }
            if(listtt.size()!=0){
                list1.add(listtt);
            }
        }

        List<Set<String>> list2 = new LinkedList<Set<String>>();
        for(PushedArtical x:pushedartical){
            Set<String> listtt = new HashSet<String>();
            for (Term temp : ToAnalysis.parse(x.gettitle())) {
                listtt.add(temp.getName());
            }
            if(listtt.size()!=0){
                list2.add(listtt);
            }
        }


        List<Set<String>> list3 = new LinkedList<Set<String>>();
        for(UserHistory x:userhistory){
            Set<String> listtt = new HashSet<String>();
            for (Term temp : ToAnalysis.parse(x.gettitle())) {
                listtt.add(temp.getName());
            }
            if(listtt.size()!=0){
                list3.add(listtt);
            }
        }


        for(int i=0;i<list1.size();i++){
            for(Set<String> x2:list2){
                double all = 0.0;
                for(String xx:list1.get(i)){
                    if(x2.contains(xx)){
                        all++;
                    }
                }
                if((all/(Math.sqrt((double)list1.get(i).size()*x2.size())))>=fencicanshu){
                    offlineresult.get(i).setscore(0.0);
                    System.out.println("分词去重过滤！");
                    continue;
                }
            }
            for(Set<String> x3:list3){
                double all = 0.0;
                for(String xx:list1.get(i)){
                    if(x3.contains(xx)){
                        all++;
                    }
                }
                if((all/(Math.sqrt((double)list1.get(i).size()*x3.size())))>=fencicanshu){
                    offlineresult.get(i).setscore(0.0);
                    System.out.println("分词去重过滤！");
                    continue;
                }
            }
        }


        /*
        List<String> yy = new LinkedList<String>();
        for(OfflineResult x:offlineresult){
            StringBuilder x2 = new StringBuilder();
            for (Term temp : ToAnalysis.parse(x.gettitle())) {
                x2 = x2.append(" ").append(temp.getName());
            }
            yy.add(x2.toString());
        }

        List<String> temptemp1 = new LinkedList<String>();
        for(PushedArtical x:pushedartical){
            StringBuilder x2 = new StringBuilder();
            for (Term temp : ToAnalysis.parse(x.gettitle())) {
                x2 = x2.append(" ").append(temp.getName());
            }
            temptemp1.add(x2.toString());
        }*/





        /*for(String s:yy){
            for(String tt: temptemp1){
                if (this.simi.getSimilarity(s,tt) >= fencicanshu) {
                    offlineresult.get(yy.indexOf(s)).setscore(0.0);
                    break;
                }
            }
        }*/
        //userhistory去重-分词
        /*for(OfflineResult s:offlineresult) {
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
                    s.setscore(0.0);
                    break;
                }
            }
        }*/



        //fenci;


        /*
        List<String> temptemp = new LinkedList<String>();
        for(UserHistory x:userhistory){
            StringBuilder x2 = new StringBuilder();
            for (Term temp : ToAnalysis.parse(x.gettitle())) {
                x2 = x2.append(" ").append(temp.getName());
            }
            temptemp.add(x2.toString());
        }
        */




        /*for(String s:yy){
            for(String tt: temptemp){
                if (this.simi.getSimilarity(s,tt) >= fencicanshu) {
                    offlineresult.get(yy.indexOf(s)).setscore(0.0);
                    break;
                }
            }
        }*/
        System.out.println(System.currentTimeMillis());


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


        collector.emit(new Values(input.getValueByField("return-info"),uin,offlineresult,input.getValueByField("save")));
    }



    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("return-info","uin","offlineresult","save"));
    }
}
