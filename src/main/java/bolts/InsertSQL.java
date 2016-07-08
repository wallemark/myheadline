package bolts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Created by Administrator on 2016/7/8.
 */
public class InsertSQL {
    public static void main(String[] args){
        String url = "jdbc:mysql://localhost:3306/work?autoReconnect=true&useSSL=false" ;
        String username = "root" ;
        String password = "123456" ;

        //File file = new File("C:\\Users\\Administrator\\Desktop\\mmsnsdocrp_pushed_20160707");
        //File file = new File("C:\\Users\\Administrator\\Desktop\\bizmsg_rp_20160707");
        File file = new File("C:\\Users\\Administrator\\Desktop\\mmsnsdocrp_canget_20160707");
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                //String[] temp = new String[6];
                //String[] temp = new String[11];
                String[] temp = new String[6];
                temp = tempString.split("\t");
                for(String x:temp){
                    System.out.println(x);
                }
                try {
                    Connection conn = DriverManager.getConnection(url, username, password);
                    Statement stmt = conn.createStatement();
                    //String sql = "INSERT INTO mmsnsdocrp_pushed VALUES (\""+temp[0]+"\","+Integer.parseInt(temp[1])+",\""+temp[2]+"\",\""+temp[3]+"\","+Integer.parseInt(temp[4])+",\""+temp[5]+"\")";
                    //String sql = "INSERT INTO bizmsg_rp VALUES (\""+temp[0]+"\","+Integer.parseInt(temp[1])+",\""+temp[2]+"\",\""+temp[3]+"\",\""+temp[4]+"\","+Float.parseFloat(temp[5])+","+Float.parseFloat(temp[6])+", "+Float.parseFloat(temp[7])+", "+Float.parseFloat(temp[8])+", "+Integer.parseInt(temp[9])+", "+Integer.parseInt(temp[10])+")";
                    String sql = "INSERT INTO mmsnsdocrp_canget VALUES (\""+temp[0]+"\","+Integer.parseInt(temp[1])+",\""+temp[2]+"\",\""+temp[3]+"\",\""+temp[4]+"\",\""+temp[5]+"\")";
                    System.out.println(sql);
                    stmt.executeUpdate(sql);
                    stmt.close();
                    conn.close();
                }catch(Exception x){
                    System.out.println(x);
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

}
