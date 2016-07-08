package bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2016/7/6.
 */
public class GetOfflineResultBolt extends BaseBasicBolt {

    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        int uin = input.getInteger(0);
        String url = "jdbc:mysql://localhost:3306/work?autoReconnect=true&useSSL=false" ;
        String username = "root" ;
        String password = "123456" ;
        List<OfflineResult> res = new LinkedList<OfflineResult>();
        try{
            Connection conn = DriverManager.getConnection(url, username, password) ;
            Statement stmt = conn.createStatement();
            String sql = "SELECT id_,title_,url_,rp_score_,topic_ FROM `bizmsg_rp` WHERE uin_ = "+uin;
            ResultSet result= stmt.executeQuery(sql);
            while(result.next()){
                OfflineResult offlineresult = new OfflineResult();
                offlineresult.setid(result.getString(1));
                offlineresult.settitle(result.getString(2));
                offlineresult.seturl(result.getString(3));
                offlineresult.setscore(result.getDouble(4));
                offlineresult.settopic(result.getInt(5));
                res.add(offlineresult);
            }
            conn.close();
        }catch(Exception se){
            System.out.println("用户历史数据读取失败！");
            se.printStackTrace() ;
        }
        collector.emit(new Values(uin,res));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin","offlineresult"));
    }
}
