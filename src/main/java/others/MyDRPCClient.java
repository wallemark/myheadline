package others;

/**
 * Created by ryanyycao on 2016/7/13.
 */


import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

public class MyDRPCClient {
    public static void main(String[] args) {
        try {
            Config conf = new Config();
            conf.setDebug(false);
            conf.put("storm.thrift.transport", "backtype.storm.security.auth.SimpleTransportPlugin");
            conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
            conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
            conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
            //conf.put(Config.DRPC_MAX_BUFFER_SIZE, 1048576);

//9800
            DRPCClient client = new DRPCClient(conf,"10.50.88.194",3774,100000);
            String result = client.execute("myheadline", "111111");

            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}