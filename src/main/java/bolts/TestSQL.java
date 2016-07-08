package bolts;

/**
 * Created by Administrator on 2016/7/7.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class TestSQL {
    public static void main(String[] args){

        String url = "jdbc:mysql://localhost:3306/work" ;
        String username = "root" ;
        String password = "123456" ;
        String id="";
        String title = "";
        int uni = 123;
        try{
            Connection conn = DriverManager.getConnection(url, username, password) ;
            Statement stmt = conn.createStatement();
            String sql = "SELECT id_,title_ FROM `mmsnsdocrp_canget` WHERE uin_ = "+uni;
            ResultSet result= stmt.executeQuery(sql);
            while(result.next()){
                id = result.getString(1);
                title = result.getString(2);
                System.out.println(id+"    "+title);
            }
            conn.close();
        }catch(Exception se){
            System.out.println("d读取失败！");
            se.printStackTrace() ;
        }
    }
}
