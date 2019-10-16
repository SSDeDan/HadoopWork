package mapReduce;

import hadoop.hdfs.PutFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ID;
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
 * @author: ZZZss
 * @Created 2019/9/29 9:25
 */
public class CalcAvgTemp  extends Configured implements Tool {


//    Map阶段
    static class CalcAvgTempMapper
            extends Mapper<LongWritable, Text,Text, DoubleWritable>{

        private RawWeatherDataParser parser=new RawWeatherDataParser();
        private Text k2=new Text();
        private  DoubleWritable v2=new DoubleWritable();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            boolean isValid = this.parser.parse(v1);
            if (isValid){
                this.k2.set(this.parser.getSid());
                this.v2.set(this.parser.getTemp());
                context.write(k2,v2);
            }
        }
    }



//  Reduce阶段
    static class CalcAvgTempReducer
            extends Reducer<Text ,DoubleWritable,Text,DoubleWritable>{

            private  Text k3=new Text();
            private  DoubleWritable v3=new DoubleWritable();


        @Override
        protected void reduce(Text k2, Iterable<DoubleWritable> v2s, Context context) throws IOException, InterruptedException {

            //求和
            double sum=0;
            //计算个数
            int count=0;

            for (DoubleWritable v2:v2s){
                sum+=v2.get();
                count++;
            }

            double avg=sum/count;
            this.k3.set(k2.toString());
            this.v3.set(avg);
            context.write(k3,v3);
        }
    }


//作业配置

    @Override
    public int run(String[] strings) throws Exception {

        //获取配置文件的信息，获取连接
        Configuration conf = this.getConf();

        //设置输入输出路径
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        //获得一个Job对象，来执行这个任务

        Job job = Job.getInstance(conf, "计算每个气象站的平均气温");
        job.setJarByClass(this.getClass());

        // Map任务的配置
            // 配置Map任务要运行的类
            // 配置Map任务输出的k2和v2的数据类型
            // 配置Map端读取原始数据的格式，
            // 配置Map任务以什么样的方式读取原始数据
            // 以下配置决定了数据进入Map端的k1和v1的数据类型
        job.setMapperClass(CalcAvgTempMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);


        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);



        //Reduce任务配置

        job.setReducerClass(CalcAvgTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);


        System.out.println("任务配置已经完成！");


        // 设置Reduce的个数
           job.setNumReduceTasks(5);

//        // 设置Combiner类，
//        job.setCombinerClass(PatentCitesReducer.class);

        return job.waitForCompletion(true)?0:1;



    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new CalcAvgTemp(),args));
    }


}
