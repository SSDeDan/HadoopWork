package mapReduce.jc;

import mapReduce.jc.CalcMaxTemp;
import mapReduce.jc.CombineMaxAndAvg;
import mapReduce.jc.GetSidTemp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: bd1903.hadoop
 * @package: com.briup.bigdata.bd1903.hadoop.mr.jc
 * @filename: RunApp.java
 * @create: 2019.10.09 15:04
 * @author: Kevin
 * @description: .
 **/
public class RunApp extends Configured implements Tool{
    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new RunApp(),args));
    }

    @Override
    public int run(String[] args) throws Exception{
        Configuration conf=this.getConf();

        Path in1=new Path(conf.get("in1"));  // 作业1的输入
        Path out1=new Path(conf.get("out1"));   // 作业1输出、作业2和3的输入
        Path out2=new Path(conf.get("out2")); // 作业2的输出，作业4的输入
        Path out3=new Path(conf.get("out3")); // 作业3的输出，作业4的输入
        Path out4=new Path(conf.get("out4")); // 作业4的输出

        // 作业1的配置
        Job job1=Job.getInstance(conf,"作业1：获取Sid和Temp");
        job1.setJarByClass(this.getClass());

        job1.setMapperClass(GetSidTemp.GetSidTempMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1,in1);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1,out1);

        // ------------------------------------------------

        // 作业2配置
        Job job2=Job.getInstance(conf,"作业2：计算最高温度");
        job2.setJarByClass(this.getClass());

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job2,out1);

        job2.setReducerClass(CalcMaxTemp.CalcMaxTempReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job2,out2);

        // -----------------------------------------

        // 作业3配置
        Job job3=Job.getInstance(conf,"作业3：计算平均温度");
        job3.setJarByClass(this.getClass());

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job3,out1);

        job3.setReducerClass(CalcAvgTemp.CalcAvgTempReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job3,out3);

        // --------------------------------------------

        // 作业4配置
        Job job4=Job.getInstance(conf,"作业4：连接最大值和平均值");
        job4.setJarByClass(this.getClass());

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job4,out2);
        FileInputFormat.addInputPath(job4,out3);

        job4.setReducerClass(CombineMaxAndAvg.CombineMaxAndAvgReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(NullWritable.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job4,out4);

        // -------------------------------------------------

        ControlledJob cj1=new ControlledJob(conf);
        cj1.setJob(job1);

        ControlledJob cj2=new ControlledJob(conf);
        cj2.setJob(job2);

        ControlledJob cj3=new ControlledJob(conf);
        cj3.setJob(job3);

        ControlledJob cj4=new ControlledJob(conf);
        cj4.setJob(job4);

        cj2.addDependingJob(cj1);

        cj3.addDependingJob(cj1);

        cj4.addDependingJob(cj2);
        cj4.addDependingJob(cj3);

        JobControl jc=new JobControl("作业流控制");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);

        // 提交作业
        Thread t=new Thread(jc);
        t.start();

        do{
            for(ControlledJob j: jc.getRunningJobList()){
                j.getJob().monitorAndPrintJob();
            }
        }while(!jc.allFinished());

        return 0;
    }
}
