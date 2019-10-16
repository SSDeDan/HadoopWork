package mapReduce.map_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 *
 *      预处理数据
 *
 * @author: ZZZss
 * @Created 2019/10/8 14:50
 */
public class HandlePre extends Configured implements Tool {




    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();

        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));


        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,",");


        Job job = Job.getInstance(conf, "预处理数据");
        job.setJarByClass(this.getClass());



        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job,in);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new HandlePre(),args));
    }
}
