package bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */
public class RegularMatchBolt extends BaseBasicBolt {

    double biaotidangguolv1 = 0.0;
    double biaotidangguolv2 = 0.0;
    double biaotidangguolv3 = 0.0;
    String s1;
    String s2;
    Pattern r1;
    Pattern r2;

    public void prepare(Map config, TopologyContext contex){
        this.biaotidangguolv1 = Double.parseDouble(config.get("biaotidangguolv1").toString());
        this.biaotidangguolv2 = Double.parseDouble(config.get("biaotidangguolv2").toString());
        this.biaotidangguolv3 = Double.parseDouble(config.get("biaotidangguolv3").toString());
        //this.s1 = config.get("regular1").toString();
        //this.s2 = config.get("regular2").toString();
        this.s1 = "(！！)|(竟然)|(震惊)|(再不看)|(一定要)|(疯转)|(转疯)|(疯传)|(传疯)|(抽奖)|(报名)|(必看)|(看完都)|(太可怕了)|(别进来)|(居然是)|(!!)|(%.?.?.?.?人)|(你敢.?.?吗)|(中奖)|(千万不要)";
        this.s2 = "(。。)|(！)|(\\.\\.\\.)|(…)|(!)";
        r1 = Pattern.compile(s1);
        r2 = Pattern.compile(s2);
    }


    public void cleanup() {
    }


    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("regularmatch start!"+System.currentTimeMillis());
        long uin = input.getLongByField("uin");
        List<OfflineResult> offlineresult = (LinkedList<OfflineResult>) input.getValueByField("offlineresult");
        for(OfflineResult x:offlineresult){
            Matcher m1 = r1.matcher(x.gettitle());
            if(m1.find()&&m1.start()==0){
                x.setscore(x.getscore()*biaotidangguolv1);
                //System.out.println("*****");
            }else if(m1.find()&&m1.start()>0){
                x.setscore(x.getscore()*biaotidangguolv2);
                //System.out.println("+++++");
            }else{
                Matcher m2 = r2.matcher(x.gettitle());
                if(m2.find()){
                    x.setscore(x.getscore()*biaotidangguolv3);
                    //System.out.println("-----");
                }
            }
        }
        System.out.println("regularmatch end!"+System.currentTimeMillis());
        collector.emit(new Values(input.getValueByField("return-info"), uin, offlineresult,input.getValueByField("save")));
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("return-info","uin", "Regularmatch","save"));
    }
}
