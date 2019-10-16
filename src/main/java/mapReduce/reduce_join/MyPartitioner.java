package mapReduce.reduce_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/10/9 21:30
 */
public class MyPartitioner extends Partitioner<KeyFlag, Text> {
    @Override
    public int getPartition(KeyFlag keyFlag, Text text, int i) {
        return keyFlag.getKey().hashCode()%i;
    }
}
