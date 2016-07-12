package bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Created by Ryan on 2016/7/12.
 */
public class FilterIdTopicBolt extends BaseBasicBolt {

    public void cleanup() {}


    public void execute(Tuple input, BasicOutputCollector collector) {
        int uin = input.getIntegerByField("uin");
        List<OfflineResult> offlineresult = (LinkedList<OfflineResult>)input.getValueByField("Regularmatch");

        //公众号相同-过滤；
        Map<String,List<OfflineResult>> temp = new HashMap<String,List<OfflineResult>>();
        for(OfflineResult x: offlineresult){
            if(temp.containsKey(x.getgongzhongid())){
                temp.get(x.getgongzhongid()).add(x);
            }else{
                List<OfflineResult> tt = new LinkedList<OfflineResult>();
                tt.add(x);
                temp.put(x.getgongzhongid(),tt);
            }
        }
        List<OfflineResult> res1 = new LinkedList<OfflineResult>();
        for(Map.Entry<String,List<OfflineResult>> entry: temp.entrySet()){
            if(entry.getValue().size()==1){
                res1.add(entry.getValue().get(0));
            }else{
                double max = Integer.MIN_VALUE;
                for(OfflineResult x:entry.getValue()){
                    if(x.getscore()>max){
                        max = x.getscore();
                    }
                }
                for(OfflineResult x:entry.getValue()){
                    if(x.getscore()!=max){
                        x.setscore(x.getscore()*0.5);
                    }
                    res1.add(x);
                }
            }
        }

        //topic相同-过滤；
        Map<Integer,List<OfflineResult>> temp2 = new HashMap<Integer,List<OfflineResult>>();
        for(OfflineResult x: res1){
            if(temp2.containsKey(x.gettopic())){
                temp2.get(x.gettopic()).add(x);
            }else{
                List<OfflineResult> tt = new LinkedList<OfflineResult>();
                tt.add(x);
                temp2.put(x.gettopic(),tt);
            }
        }
        List<OfflineResult> res2 = new LinkedList<OfflineResult>();
        for(Map.Entry<Integer,List<OfflineResult>> entry: temp2.entrySet()){
            if(entry.getValue().size()==1){
                res2.add(entry.getValue().get(0));
            }else{
                double max = Integer.MIN_VALUE;
                for(OfflineResult x:entry.getValue()){
                    if(x.getscore()>max){
                        max = x.getscore();
                    }
                }
                for(OfflineResult x:entry.getValue()){
                    if(x.getscore()!=max){
                        x.setscore(x.getscore()*0.618);
                    }
                    res2.add(x);
                }
            }
        }



        collector.emit(new Values(uin,res2));
    }



    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin", "Filter_id_topic"));
    }
}
