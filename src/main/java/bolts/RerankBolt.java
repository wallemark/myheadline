package bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2016/7/6.
 */
public class RerankBolt extends BaseBasicBolt {

    public void cleanup() {}


    public void execute(Tuple input, BasicOutputCollector collector) {
        int uin = input.getIntegerByField("uin");
        System.out.println(uin);
        List<OfflineResult> offlineresult = (LinkedList<OfflineResult>)input.getValueByField("Filter_id_topic");
        for(OfflineResult x:offlineresult){
            System.out.print(x.getid()+"     ");
            System.out.print(x.getscore()+"     ");
            System.out.print(x.gettitle()+"     ");
            System.out.print(x.gettopic()+"     ");
            System.out.println(x.geturl());
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
