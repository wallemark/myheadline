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

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class UserHistoryBolt extends BaseBasicBolt {
    String url;
    String username;
    String password;
    java.text.SimpleDateFormat sdf;
    java.util.Calendar cal;
    Connection conn;
    Statement stmt;

    public void prepare(Map config, TopologyContext contex){
        this.url = config.get("url").toString();
        this.username = config.get("username").toString();
        this.password = config.get("password").toString();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.conn = DriverManager.getConnection(url, username, password);
            this.stmt = conn.createStatement();
        }catch(Exception x){
            x.printStackTrace();
        }
    }

    public void cleanup() {
        try {
            stmt.close();
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        //int uin = input.getInteger(0);
        long uin = input.getLongByField("uin");
        List<UserHistory> res = new LinkedList<UserHistory>();

        this.sdf = new java.text.SimpleDateFormat("yyyyMMdd");
        this.cal = java.util.Calendar.getInstance();
        cal.add(java.util.Calendar.DATE,-6);
        String flag = sdf.format(cal.getTime());
        cal.add(java.util.Calendar.DATE,+6);

        try {
            while (Integer.parseInt(sdf.format(cal.getTime())) >= Integer.parseInt(flag)) {
                String date = sdf.format(cal.getTime());
                String sql = "SELECT id_,title_ FROM `mmsnsdocrp_canget` WHERE (uin_ = " + uin + ") AND (ds_ = \"" + date + "\")";
                System.out.println(sql);
                ResultSet result = stmt.executeQuery(sql);
                while (result.next()) {
                    UserHistory userhistory = new UserHistory();
                    userhistory.setid(result.getString(1));
                    userhistory.settitle(result.getString(2));
                    res.add(userhistory);
                }
                cal.add(java.util.Calendar.DATE, -1);
            }
        }catch(Exception e){
            e.printStackTrace();
        }


        collector.emit(new Values(uin,res));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin","userhistory"));
    }
}
