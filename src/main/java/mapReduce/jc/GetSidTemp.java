package mapReduce.jc;

import java.io.IOException;

import mapReduce.RawWeatherDataParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: bd1903.hadoop
 * @package: com.briup.bigdata.bd1903.hadoop.mr.jc
 * @filename: GetSidTemp.java
 * @create: 2019.10.09 14:46
 * @author: Kevin
 * @description: .
 **/
public class GetSidTemp extends Configured implements Tool{
    static class GetSidTempMapper
            extends Mapper<LongWritable,Text,Text,DoubleWritable>{
        private RawWeatherDataParser parser=new RawWeatherDataParser();
        private Text k2=new Text();
        private DoubleWritable v2=new DoubleWritable();

        @Override
        protected void map(LongWritable k1,Text v1,Context context) throws IOException, InterruptedException{
            if(this.parser.parse(v1)){
                this.k2.set(this.parser.getSid());
                this.v2.set(this.parser.getTemp());
                context.write(this.k2,this.v2);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        Configuration conf=this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));

        Job job=Job.getInstance(conf,"作业1：获取Sid和Temp");
        job.setJarByClass(this.getClass());

        job.setMapperClass(GetSidTempMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new GetSidTemp(),args));
    }
}
