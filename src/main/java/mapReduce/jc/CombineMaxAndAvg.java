package mapReduce.jc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: bd1903.hadoop
 * @package: com.briup.bigdata.bd1903.hadoop.mr.jc
 * @filename: CombineMaxAndAvg.java
 * @create: 2019.10.09 14:58
 * @author: Kevin
 * @description: .
 **/
public class CombineMaxAndAvg extends Configured implements Tool{
    static class CombineMaxAndAvgReducer
            extends Reducer<Text,Text,Text,NullWritable>{

        private Text k3=new Text();
        private NullWritable v3=NullWritable.get();

        @Override
        protected void reduce(Text k2,Iterable<Text> v2s,Context context) throws IOException, InterruptedException{
            StringBuilder sb=new StringBuilder();
            sb.append(k2.toString()).append(",");
            v2s.forEach(v2->sb.append(v2.toString()).append(","));
            this.k3.set(sb.substring(0,sb.length()-1));
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        Configuration conf=this.getConf();
        Path in1=new Path(conf.get("in1"));
        Path in2=new Path(conf.get("in2"));
        Path out=new Path(conf.get("out"));

        Job job=Job.getInstance(conf,"作业4：连接最大值和平均值");
        job.setJarByClass(this.getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job,in1);
        FileInputFormat.addInputPath(job,in2);

        job.setReducerClass(CombineMaxAndAvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new CombineMaxAndAvg(),args));
    }
}
