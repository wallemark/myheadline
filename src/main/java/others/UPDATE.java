package others;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by Administrator on 2016/7/14.
 */
public class UPDATE {
    public static void main(String[] args){

        String url = "jdbc:mysql://localhost:3306/work?autoReconnect=true&useSSL=false" ;
        String username = "root" ;
        String password = "123456" ;
        try{
            Connection conn = DriverManager.getConnection(url, username, password) ;
            Statement stmt = conn.createStatement();
            String sql = "UPDATE mmsnsdocrp_canget SET ds_ = 20160714";
            stmt.executeUpdate(sql);
            conn.close();
        }catch(Exception se){
            System.out.println("d读取失败！");
            se.printStackTrace() ;
        }
    }
}
