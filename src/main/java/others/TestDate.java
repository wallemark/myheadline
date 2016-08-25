package others;

import java.util.Calendar;

/**
 * Created by Administrator on 2016/7/14.
 */
public class TestDate {
    public static void main(String[] args){
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyyMMdd");
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.add(java.util.Calendar.DATE,0);
        System.out.println(sdf.format(cal.getTime()));
        cal.add(java.util.Calendar.DATE,-1);
        System.out.println(sdf.format(cal.getTime()));
        cal.add(java.util.Calendar.DATE,-1);
        System.out.println(sdf.format(cal.getTime()));
        cal.add(java.util.Calendar.DATE,-1);
        System.out.println(sdf.format(cal.getTime()));
        cal.add(java.util.Calendar.DATE,-1);
        System.out.println(sdf.format(cal.getTime()));
        cal.add(java.util.Calendar.DATE,-1);
        System.out.println(sdf.format(cal.getTime()));
        cal.add(java.util.Calendar.DATE,-1);
        System.out.println(sdf.format(cal.getTime()));
        //System.out.println(""+cal.get(Calendar.YEAR)+cal.get(Calendar.MONTH+1)+cal.get(Calendar.DAY_OF_MONTH));
    }
}
