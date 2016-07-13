package spouts;

/**
 * Created by Administrator on 2016/7/6.
 * Edit by ryanyycao
 */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class DrpcSpout extends BaseRichSpout {


    private SpoutOutputCollector collector;
    private FileReader fileReader;
    public void ack(Object msgId) {
        System.out.println("OK:"+msgId);
    }
    public void close() {}
    public void fail(Object msgId) {
        System.out.println("FAIL:"+msgId);
    }

    public void nextTuple() {

        String str;
        //Open the reader
        BufferedReader reader = new BufferedReader(fileReader);
        try{
            //Read all lines
            while((str = reader.readLine()) != null){
                this.collector.emit(new Values(Integer.parseInt(str)));
                Thread.sleep(2000);
            }
        }catch(Exception e){
            throw new RuntimeException("Error reading tuple",e);
        }
    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
        }
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uin"));
    }
}
