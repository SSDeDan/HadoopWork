package mrHomework;

import mapReduce.RawWeatherDataParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * @author: ZZZss
 * @Created 2019/9/29 12:55
 */
public class weatherMapReduce extends Configured implements Tool {


    //map阶段
    static  class weatherMapReduceMapper
            extends Mapper<LongWritable, Text,Text,Text>{

        private RawWeatherDataParser parser=new RawWeatherDataParser();
        private  Text k2=new Text();
        private  Text v2=new Text();


        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            boolean isValid = this.parser.parse(v1);
            if (isValid){
                k2.set(String.valueOf(this.parser.getYear()));

                StringBuffer sb = new StringBuffer();
                sb.append(this.parser.getSid())
                        .append("\t")
                        .append(this.parser.getTemp());
                v2.set(String.valueOf(sb));
                context.write(k2,v2);

            }
        }
    }


    //reduce阶段

    static class weatherMapReduceReducer extends Reducer<Text,Text,Text,Text>{

        private  Text k3=new Text();
        private  Text v3=new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {

        for (Text v2:v2s){
            k3.set(k2);
            k3.set(v2);
            context.write(k3,v3);

        }

        }
    }


    //配置阶段在run方法里面配置
    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf=this.getConf();

        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        //获取job对象
        Job job = Job.getInstance(conf, "数据最小的五年数据");
        job.setJarByClass(this.getClass());


        //map任务的配置
        job.setMapperClass(weatherMapReduceMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输入路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        //reduce任务配置
        job.setReducerClass(weatherMapReduceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        //设置输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        System.out.println("任务配置完成！");



        //设置reduce的个数
//        job.setNumReduceTasks(5);

        //设置Combiner类
//        job.setCombinerClass(PatentCitesReducer.class);


        return job.waitForCompletion(true)?0:1;



    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new weatherMapReduce(),args));

    }

}
