package mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/29 12:29
 *
 *      生成三列天气数据
 *
 *
 */
public class YearSidTemp  extends Configured implements Tool {


        static class YearSidTempMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
            private RawWeatherDataParser parser=new RawWeatherDataParser();
            private IntWritable k2=new IntWritable();
            private Text v2=new Text();

            @Override
            protected void map(LongWritable k1,Text v1,Context context) throws IOException, InterruptedException{
                if(this.parser.parse(v1)){
                    this.k2.set(this.parser.getYear());
                    this.v2.set(this.parser.getSid()+"\t"+this.parser.getTemp());
                    context.write(this.k2,this.v2);
                }
            }
        }




        @Override
        public int run(String[] strings) throws Exception{
            Configuration conf=this.getConf();
            Path in=new Path(conf.get("in"));
            Path out=new Path(conf.get("out"));

            String outputType=conf.get("output.type");

            Job job=Job.getInstance(conf,"生成数据集");
            job.setJarByClass(this.getClass());

            job.setMapperClass(YearSidTempMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            // TextInputFormat.addInputPath(job,in);
            FileInputFormat.addInputPath(job,in);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        if("seq".equals(outputType)) job.setOutputFormatClass(SequenceFileOutputFormat.class);
        else job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);

//        job.setPartitionerClass(HashPartitioner.class);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new YearSidTemp(),args));

    }
}
