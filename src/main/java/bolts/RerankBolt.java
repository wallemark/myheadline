package bolts;

import MMDCMYHEADLINE.MmdcmyheadlineCgi;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.xerces.impl.dv.util.Base64;
import org.apache.storm.task.TopologyContext;
import java.util.*;
import java.sql.*;

/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */
public class RerankBolt extends BaseBasicBolt {
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
        long uin = input.getLongByField("uin");
        List<OfflineResult> offlineresult = (LinkedList<OfflineResult>)input.getValueByField("Filter_id_topic");
        OfflineResult[] res = new OfflineResult[offlineresult.size()];
        for(int i=0;i<res.length;i++){
            res[i] = offlineresult.get(i);
        }
        Arrays.sort(res, new MyComprator());



        //jiluyituisong
        String pushedid = ""+uin+"_" + System.currentTimeMillis();

        try{
            if(input.getValueByField("save").equals("true")) {
                this.sdf = new java.text.SimpleDateFormat("yyyyMMdd");
                this.cal = java.util.Calendar.getInstance();
                cal.add(java.util.Calendar.DATE,0);
                String date = sdf.format(cal.getTime());
                for (int i = 0; i < res.length && i < 5; i++) {
                    String sql = "INSERT INTO mmsnsdocrp_pushed VALUES (\"" + date + "\"," + uin + ",\"" + res[i].getid() + "\",\"" + pushedid + "\"," + 1 + ",\"" + res[i].gettitle() + "\")";
                    System.out.println(sql);
                    stmt.executeUpdate(sql);
                }
            }

        }catch(Exception e){
            e.printStackTrace();
        }


        //jilupushed
        /*try{
            this.sdf = new java.text.SimpleDateFormat("yyyyMMdd");
            this.cal = java.util.Calendar.getInstance();
            cal.add(java.util.Calendar.DATE,0);
            String date = sdf.format(cal.getTime());

            for(int i=0;i<res.length&&i<5;i++){
                String sql = "INSERT INTO mmsnsdocrp_pushed VALUES (\""+date+"\","+ uin +",\""+res[i].getid()+"\",\""+"123456"+"\","+ 1 +",\""+res[i].gettitle()+"\")";
                System.out.println(sql);
                stmt.executeUpdate(sql);
            }
        }catch(Exception e){
            e.printStackTrace();
        }*/





        //序列化
        MmdcmyheadlineCgi.MMDCMyHeadlineResp.Builder Resbuild = MmdcmyheadlineCgi.MMDCMyHeadlineResp.newBuilder();
        List<MmdcmyheadlineCgi.ArticleInfo> articallist = new LinkedList<MmdcmyheadlineCgi.ArticleInfo>();
        for(int i=0;i<res.length&&i<5;i++){
            MmdcmyheadlineCgi.ArticleInfo.Builder temp = MmdcmyheadlineCgi.ArticleInfo.newBuilder();
            temp.setDate(Integer.parseInt(res[i].getdate()));
            temp.setDocId(res[i].getid());
            temp.setRelationNum(0);
            temp.setIcon("");
            temp.setTitle(res[i].gettitle());
            temp.setRank(i+1);
            temp.setSource(1);
            temp.setTopic(String.valueOf(res[i].gettopic()));
            temp.setUrl(res[i].geturl());
            temp.setStrategyId(4);
            temp.setDebugInfo(String.valueOf(res[i].getscore()));
            articallist.add(temp.build());
        }
        Resbuild.addAllArticleList(articallist);
        Resbuild.setUin(uin);
        Resbuild.setPushId(pushedid);
        if(articallist.size()==0){
            Resbuild.setErrorCode(MmdcmyheadlineCgi.MMDCMyHeadlineErrorCode.MYHEADLINE_UINNOTFOUND);
        }else{
            Resbuild.setErrorCode(MmdcmyheadlineCgi.MMDCMyHeadlineErrorCode.MYHEADLINE_OK);
        }
        MmdcmyheadlineCgi.MMDCMyHeadlineResp xxg = Resbuild.build();

        // for(int i=0;i<5;i++){
        //System.out.print(x.getid()+"     ");
        //System.out.print(x.getscore()+"     ");
        //System.out.print(res[i].gettitle()+"     ");
        //System.out.print(x.gettopic()+"     ");
        //System.out.println(x.geturl());
        //}
        System.out.println(xxg);
        try {
            String res1 = Base64.encode(xxg.toByteArray());
            collector.emit(new Values(res1,input.getValueByField("return-info")));
            //collector.emit(new Values(xxg.toByteArray(),input.getValueByField("return-info")));
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin","return-info"));
    }


    class MyComprator implements Comparator {
        public int compare(Object arg0, Object arg1) {
            if(((OfflineResult)arg0).getscore()>((OfflineResult)arg1).getscore()){
                return -1;
            }else if(((OfflineResult)arg0).getscore()==((OfflineResult)arg1).getscore()) {
                return 0;
            }else{
                return 1;
            }
        }

    }
}
