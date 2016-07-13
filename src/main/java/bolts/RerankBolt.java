package bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Arrays;

/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */
public class RerankBolt extends BaseBasicBolt {

    public void cleanup() {}


    public void execute(Tuple input, BasicOutputCollector collector) {
        int uin = input.getIntegerByField("uin");
        System.out.println(uin);
        List<OfflineResult> offlineresult = (LinkedList<OfflineResult>)input.getValueByField("Filter_id_topic");
        OfflineResult[] res = new OfflineResult[offlineresult.size()];
        for(int i=0;i<res.length;i++){
            res[i] = offlineresult.get(i);
        }
        Arrays.sort(res, new MyComprator());

        for(OfflineResult x:res){
            System.out.print(x.getid()+"     ");
            System.out.print(x.getscore()+"     ");
            System.out.print(x.gettitle()+"     ");
            System.out.print(x.gettopic()+"     ");
            System.out.println(x.geturl());
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
    class MyComprator implements Comparator {
        public int compare(Object arg0, Object arg1) {
            if(((OfflineResult)arg0).getscore()>((OfflineResult)arg1).getscore()){
                return -1;
            }else{
                return 1;
            }
        }
    }
}
