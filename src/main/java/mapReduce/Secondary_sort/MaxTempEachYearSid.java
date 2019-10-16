package mapReduce.Secondary_sort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 *
 *          计算每一年每个气象站的温度
 *          使用二次排序
 *          即第一列按照年份排序
 *              第二列按照气象站编号排序
 *
 * @author: ZZZss
 * @Created 2019/10/9 10:16
 */
public class MaxTempEachYearSid extends Configured implements Tool {


    static class MaxTempEachYearSidMapper
            extends Mapper<LongWritable, Text, YearSid, DoubleWritable> {

        private  YearSid k2=new YearSid();
        private  DoubleWritable v2=new DoubleWritable();


        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("[\t]");

            k2.setYear(Integer.parseInt(strs[0]));

            k2.setSid(strs[1]);

            v2.set(Double.parseDouble(strs[2]));

            context.write(k2,v2);

        }
    }


    static class MaxTempEachYearSidReducer
            extends Reducer< YearSid, DoubleWritable, YearSid, DoubleWritable> {

        private  YearSid k3=new YearSid();
        private  DoubleWritable v3=new DoubleWritable();


        @Override
        protected void reduce(YearSid k2, Iterable<DoubleWritable> v2s, Context context) throws IOException, InterruptedException {

            for (DoubleWritable v2 : v2s) {

                this.k3.setYear(k2.getYear());
                this.k3.setSid(k2.getSid());
                this.v3.set(v2.get());
                context.write(k3,v3);
            }



        }
    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf,"二次排序");
        job.setJarByClass(this.getClass());


        job.setMapperClass(MaxTempEachYearSidMapper.class);
        job.setMapOutputKeyClass(YearSid.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(MaxTempEachYearSidReducer.class);
        job.setOutputKeyClass(YearSid.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        //设置reduec的个数
        job.setNumReduceTasks(5);

        //设置分区
        job.setPartitionerClass(MyPartitioner.class);

        //自设置分组比较器
        job.setGroupingComparatorClass(MyComparator.class);


        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MaxTempEachYearSid(),args));
    }
}
