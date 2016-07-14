package bolts;

import MMDCMYHEADLINE.MmdcmyheadlineCgi;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */
public class RerankBolt extends BaseBasicBolt {

    public void cleanup() {}


    public void execute(Tuple input, BasicOutputCollector collector) {
        int uin = input.getIntegerByField("uin");
        List<OfflineResult> offlineresult = (LinkedList<OfflineResult>)input.getValueByField("Filter_id_topic");
        OfflineResult[] res = new OfflineResult[offlineresult.size()];
        for(int i=0;i<res.length;i++){
            res[i] = offlineresult.get(i);
        }
        Arrays.sort(res, new MyComprator());

        //序列化
        MmdcmyheadlineCgi.MMDCMyHeadlineResp.Builder Resbuild = MmdcmyheadlineCgi.MMDCMyHeadlineResp.newBuilder();
        List<MmdcmyheadlineCgi.ArticleInfo> articallist = new LinkedList<MmdcmyheadlineCgi.ArticleInfo>();
        for(int i=0;i<5;i++){
            MmdcmyheadlineCgi.ArticleInfo.Builder temp = MmdcmyheadlineCgi.ArticleInfo.newBuilder();
            temp.setDate(111);
            temp.setDocId(res[i].getid());
            temp.setRelationNum(111);
            temp.setIcon("111");
            temp.setRank(i+1);
            temp.setSource(111);
            temp.setTopic(String.valueOf(res[i].gettopic()));
            temp.setUrl(res[i].geturl());
            temp.setDebugInfo("111");
            articallist.add(temp.build());
        }
        Resbuild.addAllArticleList(articallist);
        Resbuild.setUin(uin);
        MmdcmyheadlineCgi.MMDCMyHeadlineResp xxg = Resbuild.build();

        /*for(OfflineResult x:resreturn){
            System.out.print(x.getid()+"     ");
            System.out.print(x.getscore()+"     ");
            System.out.print(x.gettitle()+"     ");
            System.out.print(x.gettopic()+"     ");
            System.out.println(x.geturl());
        }*/

        collector.emit(new Values(xxg.toString(),input.getValueByField("return-info")));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin","return-info"));
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
