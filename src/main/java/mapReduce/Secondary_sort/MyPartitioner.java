package mapReduce.Secondary_sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 *
 *      自定义分区器
 *
 * @author: ZZZss
 * @Created 2019/10/9 11:08
 */
public class MyPartitioner extends Partitioner<YearSid, DoubleWritable> {

    @Override
    public int getPartition(
            YearSid yearSid,
            DoubleWritable doubleWritable,
            int i
    ) {

        IntWritable year = yearSid.getYear();

        return year.hashCode()%i;       //返回值就是Reduce的编号
    }
}
