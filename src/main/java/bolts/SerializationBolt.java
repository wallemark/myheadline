package bolts;

import MMDCMYHEADLINE.MmdcmyheadlineCgi;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.xerces.impl.dv.util.Base64;


/**
 *  * Created by ryanyycao on 2016/7/18.
 *   */
public class SerializationBolt extends BaseBasicBolt {

    public void cleanup() {}


    public void execute(Tuple input, BasicOutputCollector collector) {
        try{
            System.out.println(input.getString(0));
            System.out.println(input.getValue(1));
            System.out.println(input.getString(0).getBytes());
            //  MmdcmyheadlineCgi.MMDCMyHeadlineReq res = MmdcmyheadlineCgi.MMDCMyHeadlineReq.parseFrom(input.getString(0).getBytes());

            MmdcmyheadlineCgi.MMDCMyHeadlineReq res = MmdcmyheadlineCgi.MMDCMyHeadlineReq.parseFrom(Base64.decode(input.getString(0)));
            System.out.println(res);
            String s;
            if(res.getSave2PushedFlag()==true){
                s = "true";
            }else{
                s = "false";
            }
            collector.emit(new Values(input.getValueByField("return-info"),res.getUin(),s));
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("return-info","uin","save"));
    }
}

