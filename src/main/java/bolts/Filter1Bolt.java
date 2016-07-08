package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by Administrator on 2016/7/6.
 */
public class Filter1Bolt extends BaseBasicBolt {

    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        //collector.emit(new Values(uin,id,title,sqlurl,score,topic));
        int uin = input.getInteger(0);
        collector.emit(new Values(uin));
    }



    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin+4"));
    }
}
