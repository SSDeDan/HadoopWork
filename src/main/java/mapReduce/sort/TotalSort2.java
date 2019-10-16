package mapReduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/10/8 23:25
 */
public class TotalSort2 extends Configured implements Tool {

    static class TotalSort2Mapper
            extends Mapper<LongWritable, Text,LongWritable, DoubleWritable> {

        private LongWritable k2=new LongWritable();
        private  DoubleWritable v2=new DoubleWritable();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("[\t]");
            this.k2.set(Integer.parseInt(strs[0]));
            this.v2.set(Double.parseDouble(strs[2]));
            context.write(this.k2,this.v2);
        }
    }

    static class TotalSort2Reducer
            extends Reducer< LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

        private LongWritable k3=new LongWritable();
        private  DoubleWritable v3=new DoubleWritable();
        @Override
        protected void reduce(LongWritable k2, Iterable<DoubleWritable> v2s, Context context) throws IOException, InterruptedException {

            double max =-Double.MAX_VALUE;

            for (DoubleWritable v2 : v2s) {
                double val = v2.get();
                if (val>max)max=val;
            }
            this.k3.set(k2.get());
            this.v3.set(max);
            context.write(this.k3,this.v3);

        }
    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf,"全局排序");
        job.setJarByClass(this.getClass());


        job.setMapperClass(TotalSort2Mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(TotalSort2Reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        //设置reduce个数
        job.setNumReduceTasks(3);

        //更改分区器
        job.setPartitionerClass(TotalOrderPartitioner.class);

        //产生采样数据
        InputSampler.RandomSampler<LongWritable, DoubleWritable> sampler =
                new InputSampler.RandomSampler(0.8, 1000, 3);

        //生成分区文件【采样文件】,在运行该程序时，
        //客户端会生成采样文件，并将其上传至hdfs集群的用户的家目录下；
        //_partition.list
        InputSampler.writePartitionFile(job,sampler);

        //获取采样文件,获取采样文件的路径
        String file = TotalOrderPartitioner.getPartitionFile(conf);

        //上传采样文件之HDFS集群
        job.addCacheFile(new URI(file));



        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TotalSort(),args));
    }
}
