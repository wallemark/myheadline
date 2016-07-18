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
 * Created by Administrator on 2016/7/18.
 */
public class SerializationBolt extends BaseBasicBolt {

    public void cleanup() {}


    public void execute(Tuple input, BasicOutputCollector collector) {
        //反序列化
        try{

            //byte[] s = (byte[])input.getValue(0);
            //System.out.println((String)input.getValue(0));
            //MmdcmyheadlineCgi.MMDCMyHeadlineReq res = MmdcmyheadlineCgi.MMDCMyHeadlineReq.parseFrom(((String)input.getValue(0)).getBytes());
            //MmdcmyheadlineCgi.MMDCMyHeadlineReq res = MmdcmyheadlineCgi.MMDCMyHeadlineReq.parseFrom(((String)input.getValueByField("uin")).getBytes());
            MmdcmyheadlineCgi.MMDCMyHeadlineReq res = MmdcmyheadlineCgi.MMDCMyHeadlineReq.parseFrom(Base64.decode(input.getString(0)));
            System.out.println(res);
            collector.emit(new Values(input.getValueByField("return-info"),res.getUin()));
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("return-info","uin"));
    }
}
