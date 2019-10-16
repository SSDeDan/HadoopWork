package mapReduce.Secondary_sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 *
 *
 *      自定义分组比较器
 *
 *
 * @author: ZZZss
 * @Created 2019/10/9 11:14
 */
public class MyComparator extends WritableComparator {

  //必须在构造器中指定要比较的数据


    public MyComparator() {
        super(YearSid.class,true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        YearSid ysa= (YearSid) a;
        YearSid ysb= (YearSid) b;

        return  ysa.getYear().compareTo(ysb.getYear());


    }
}
