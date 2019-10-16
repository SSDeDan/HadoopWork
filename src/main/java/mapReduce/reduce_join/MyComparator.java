package mapReduce.reduce_join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/10/9 21:31
 */
public class MyComparator  extends WritableComparator {


    public MyComparator() {

        super(KeyFlag.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        KeyFlag kfa= (KeyFlag) a;
        KeyFlag kfb= (KeyFlag) b;
        return kfa.getKey().compareTo(kfb.getKey());
    }
}
