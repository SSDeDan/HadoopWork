package mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 *
 *          Mapper和Reducer的链式调用
 *
 * @author: ZZZss
 * @Created 2019/9/29 15:27
 */
//
public class ChainMapperAndReducer extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf=this.getConf();

        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,",");


        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        //获取job对象
        Job job = Job.getInstance(conf, "链式调用");
        job.setJarByClass(this.getClass());




        ChainMapper.addMapper(
                //反转
                job, InverseMapper.class,
                Text.class,Text.class,
                Text.class,Text.class,
                conf
        );


        ChainMapper.addMapper(
                //词频统计
                job,TokenCounterMapper.class,
                Text.class,Text.class,
                Text.class, IntWritable.class,
                conf
        );

        ChainReducer.setReducer(
                //计数
                job, IntSumReducer.class,
                Text.class, IntWritable.class,
                Text.class, IntWritable.class,
                conf
        );

        ChainReducer.addMapper(
                job,InverseMapper.class,
                Text.class,IntWritable.class,
                IntWritable.class,Text.class,
                conf
        );


        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job,in);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);


        return job.waitForCompletion(true)?0:1;

    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new ChainMapperAndReducer(),args));

    }

}
