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

public class PushedArticalBolt extends BaseBasicBolt {
    String url;
    String username;
    String password;
    java.text.SimpleDateFormat sdf;
    java.util.Calendar cal;

    public void prepare(Map config, TopologyContext contex){
        this.url = config.get("url").toString();
        this.username = config.get("username").toString();
        this.password = config.get("password").toString();
    }

    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        //int uin = input.getInteger(0);
        int uin = Integer.parseInt(input.getString(0));
        List<PushedArtical> res = new LinkedList<PushedArtical>();

        this.sdf = new java.text.SimpleDateFormat("yyyyMMdd");
        this.cal = java.util.Calendar.getInstance();
        cal.add(java.util.Calendar.DATE,-6);
        String flag = sdf.format(cal.getTime());
        cal.add(java.util.Calendar.DATE,+6);

        while(Integer.parseInt(sdf.format(cal.getTime()))>=Integer.parseInt(flag)) {
            String date = sdf.format(cal.getTime());
            try{
                Connection conn = DriverManager.getConnection(url, username, password) ;
                Statement stmt = conn.createStatement();
                String sql = "SELECT docid_,title_ FROM `mmsnsdocrp_pushed` WHERE (uin_ = "+uin + ") AND (ds_ = " + date + ")";
                System.out.println(sql);
                ResultSet result= stmt.executeQuery(sql);
                while(result.next()){
                    PushedArtical pushedartical = new PushedArtical();
                    pushedartical.setid(result.getString(1));
                    pushedartical.settitle(result.getString(2));
                    res.add(pushedartical);
                }
                stmt.close();
                conn.close();
                cal.add(java.util.Calendar.DATE,-1);
            }catch(Exception se){
                System.out.println("历史推送数据读取失败！");
                se.printStackTrace() ;
            }

        }
        collector.emit(new Values(uin,res));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin","pushedartical"));
    }
}