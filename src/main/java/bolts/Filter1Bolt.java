package bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2016/7/6.
 */
public class Filter1Bolt extends BaseBasicBolt {

    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        int uin = input.getIntegerByField("uin");
        List<OfflineResult> offlineresult = (LinkedList<OfflineResult>)input.getValueByField("offlineresult");
        List<PushedArtical> pushedartical = (LinkedList<PushedArtical>)input.getValueByField("pushedartical");
        List<UserHistory> userhistory = (LinkedList<UserHistory>)input.getValueByField("userhistory");
        for(OfflineResult x:offlineresult){
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
        }



        collector.emit(new Values(uin));
    }



    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin+4"));
    }
}
