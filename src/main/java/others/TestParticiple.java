package others;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;


/**
 * Created by Administrator on 2016/7/11.
 * Edit by ryanyycao
 */
public class TestParticiple {
    public static void main(String[] args){
        String str = "奔驰、丰田们为何造不出特斯拉？因为穷" ;
        StringBuilder mm = new StringBuilder();
        for(Term x:ToAnalysis.parse(str)){
            mm = mm.append(" ").append(x.getName());
        }
        System.out.println(mm);

    }
}
