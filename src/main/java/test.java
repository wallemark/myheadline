import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2016/7/12.
 */
public class test {
    public static void main(String[] arsg){
        String test = "!";
        String s1 = "(！！)|(竟然)|(震惊)|(再不看)|(一定要)|(疯转)|(转疯)|(疯传)|(传疯)|(抽奖)|(报名)|(必看)|(看完都)|(太可怕了)|(别进来)|(居然是)|(\\!\\!)|(%.?.?人)|(你敢.?.?吗)|(中奖)|(千万不要)";
        String s2 = "(。。)|(！)|(\\.\\.\\.)|(…)|(\\!)";
        Pattern r1 = Pattern.compile(s1);
        Pattern r2 = Pattern.compile(s2);
        Matcher m1 = r1.matcher(test);
        if (m1.find( )) {
            System.out.println( m1.start());
        } else {
            System.out.println("-1");
        }
        Matcher m2 = r2.matcher(test);
        if (m2.find( )) {
            System.out.println( m2.start());
        } else {
            System.out.println("-1");
        }


    }
}
