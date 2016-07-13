package bolts;

/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */
import org.apache.storm.task.TopologyContext;
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
import java.util.Map;

public class UserHistoryBolt extends BaseBasicBolt {
    String url;
    String username;
    String password;

    public void prepare(Map config, TopologyContext contex){
        this.url = config.get("url").toString();
        this.username = config.get("username").toString();
        this.password = config.get("password").toString();
    }

    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        int uin = input.getInteger(0);
        List<UserHistory> res = new LinkedList<UserHistory>();
        try{
            Connection conn = DriverManager.getConnection(url, username, password) ;
            Statement stmt = conn.createStatement();
            String sql = "SELECT id_,title_ FROM `mmsnsdocrp_canget` WHERE uin_ = "+uin;
            ResultSet result= stmt.executeQuery(sql);
            while(result.next()){
                UserHistory userhistory = new UserHistory();
                userhistory.setid(result.getString(1));
                userhistory.settitle(result.getString(2));
                res.add(userhistory);
            }
            conn.close();
        }catch(Exception se){
            System.out.println("用户历史数据读取失败！");
            se.printStackTrace() ;
        }
        collector.emit(new Values(uin,res));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin","userhistory"));
    }
}
