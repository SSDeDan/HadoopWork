package mapReduce.jc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * @filename: CalcMaxTemp.java
 * @create: 2019.10.09 14:50
 * @author: Kevin
 * @description: .
 **/
public class CalcMaxTemp extends Configured implements Tool{
    static class CalcMaxTempReducer
            extends Reducer<Text,Text,Text,DoubleWritable>{
        private Text k3=new Text();
        private DoubleWritable v3=new DoubleWritable();

        @Override
        protected void reduce(Text k2,Iterable<Text> v2s,Context context) throws IOException, InterruptedException{
            double max=-Double.MAX_VALUE;
            for(Text v2: v2s){
                double val=Double.parseDouble(v2.toString());
                if(val>max) max=val;
            }
            this.k3.set(k2.toString());
            this.v3.set(max);

            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        Configuration conf=this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));

        Job job=Job.getInstance(conf,"作业2：计算最高温度");
        job.setJarByClass(this.getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job,in);

        job.setReducerClass(CalcMaxTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new CalcMaxTemp(),args));
    }
}
