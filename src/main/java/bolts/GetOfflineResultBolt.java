package bolts;

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
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */
public class GetOfflineResultBolt extends BaseBasicBolt {
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
            //Class.forName("com.mysql.jdbc.Driver");
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
        //System.out.println(input.getValue(0));
        //System.out.println(input.getValue(1));
        //int uin = input.getInteger(0);
        //int uin = Integer.parseInt(input.getString(1));
        long uin = input.getLongByField("uin");
        List<OfflineResult> res = new LinkedList<OfflineResult>();

        //date
        this.sdf = new java.text.SimpleDateFormat("yyyyMMdd");
        this.cal = java.util.Calendar.getInstance();
        cal.add(java.util.Calendar.DATE,-6);
        String flag = sdf.format(cal.getTime());
        cal.add(java.util.Calendar.DATE,+6);
        try{
            while (res.size() < 10 && Integer.parseInt(sdf.format(cal.getTime())) >= Integer.parseInt(flag)) {
                String date = sdf.format(cal.getTime());
                String sql = "SELECT id_,title_,url_,rp_score_,topic_ FROM `bizmsg_rp` WHERE (uin_ = " + uin + ") AND (ds_ = \"" + date + "\")";
                System.out.println(sql);
                ResultSet result = stmt.executeQuery(sql);
                while (result.next()) {
                    OfflineResult offlineresult = new OfflineResult();
                    String id = result.getString(1);
                    String[] temp = id.split("_");
                    offlineresult.setgongzhongid(temp[1]);
                    offlineresult.setid(id);
                    offlineresult.settitle(result.getString(2));
                    offlineresult.seturl(result.getString(3));
                    offlineresult.setscore(result.getDouble(4));
                    offlineresult.settopic(result.getInt(5));
                    offlineresult.setdate(date);
                    res.add(offlineresult);
                }
                cal.add(java.util.Calendar.DATE, -1);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        collector.emit(new Values(input.getValueByField("return-info"),uin,res,input.getValueByField("save")));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("return-info","uin","offlineresult","save"));
    }
}
