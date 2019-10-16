package mapReduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
 * @Created 2019/10/8 22:04
 */
public class TotalSort extends Configured implements Tool {

    static class TotalSortMapper
            extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{

        private IntWritable k2=new IntWritable();
        private  DoubleWritable v2=new DoubleWritable();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("[\t]");
            this.k2.set(Integer.parseInt(strs[0]));
            this.v2.set(Double.parseDouble(strs[2]));
            context.write(this.k2,this.v2);
        }
    }

static class TotalSortReducer
        extends Reducer< IntWritable, DoubleWritable, IntWritable, DoubleWritable>{

    private IntWritable k3=new IntWritable();
    private  DoubleWritable v3=new DoubleWritable();
    @Override
    protected void reduce(IntWritable k2, Iterable<DoubleWritable> v2s, Context context) throws IOException, InterruptedException {

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


        job.setMapperClass(TotalSortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(TotalSortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        //设置reduce个数
        job.setNumReduceTasks(3);

        //更改分区器
        job.setPartitionerClass(TotalOrderPartitioner.class);

        //产生采样数据
        InputSampler.RandomSampler sampler =
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
