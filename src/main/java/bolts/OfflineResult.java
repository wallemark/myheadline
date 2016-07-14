package bolts;


/**
 * Created by Administrator on 2016/7/7.
 * Edit by ryanyycao
 */
public class OfflineResult{
    private String id;
    private String title;
    private String url;
    private double score;
    private int topic;
    private String gongzhongid;
    private String date;
    public void setid(String x){
        this.id = x;
    }
    public void settitle(String x){
        this.title = x;
    }
    public void seturl(String x){
        this.url = x;
    }
    public void setscore(double x){
        this.score = x;
    }
    public void settopic(int x){
        this.topic = x;
    }
    public void setdate(String x) { this.date = x; }
    public void setgongzhongid(String x){
        this.gongzhongid = x;
    }
    public String getid(){
        return id;
    }
    public String gettitle(){
        return title;
    }
    public String geturl(){
        return url;
    }
    public double getscore(){
        return score;
    }
    public int gettopic(){
        return topic;
    }
    public String getgongzhongid(){
        return gongzhongid;
    }
    public String getdate() { return date; }
}
