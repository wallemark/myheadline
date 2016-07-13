package others;

/**
 * Created by ryanyycao on 2016/7/13.
 */


import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

/**
 * DRPC客户端调用代码
 */
public class MyDRPCClient {

    /**
     指定DRPC地址和端口,storm.yaml文件的配置：
     drpc.servers:
     -  "drpcserver1"
     -  "drpcserver12"
     */
    public static void main(String[] args) {
        try {
            Config conf = new Config();
            DRPCClient client = new DRPCClient(conf,"127.0.0.1",3772);
            String result = client.execute("Spout", "111111");

            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}