package mapReduce;

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
 * @author: ZZZss
 * @Created 2019/9/29 14:20
 */
public class CalAvgTempUseCombiner1  extends Configured implements Tool {


    //map阶段
    static  class CalAvgTempUseCombiner1Mapper
            extends Mapper<LongWritable,Text,Text,AvgNum1>{


        //获取原始数据
        private  RawWeatherDataParser parser=new RawWeatherDataParser();
        //构造输出k2和v2的类型
        private  Text k2=new Text();
        private  AvgNum1 v2=new AvgNum1();


        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            if (parser.parse(v1)){

                //气象站编号
                this.k2.set(this.parser.getSid());
                //得到温度
                this.v2.setAvg(this.parser.getTemp());
                //出现的个数
                this.v2.setNum(1L);

                //提交
                context.write(this.k2,this.v2);

            }
        }
    }



    //Combiner阶段
    //计算平均气温

    static   class CalAvgTempUseCombiner1Combiner
            extends Reducer<Text,AvgNum1,Text,AvgNum1>{

        //设置输出的类型
        private  AvgNum1 v2c=new AvgNum1();

        @Override
        protected void reduce(Text k2c, Iterable<AvgNum1> v2cs, Context context) throws IOException, InterruptedException {

            //1.计算一共有多少个数据
            long count=0;

            //2.计算所有的数据产生的平均值
            double sum=0;

            for (AvgNum1 v2c : v2cs) {
                //获取Avg的值，因为是DoubleWritable类型，所以需要使用get方法来获取值
                sum+=v2c.getAvg().get()*v2c.getNum().get();
                count+=v2c.getNum().get();

            }

            //计算平均温度
            double avg=sum/count;


            //3.将该平均值和数据个数的总和作为value【AvgNum】交给Reducer

            this.v2c.setAvg(avg);
            this.v2c.setNum(count);

            //提交
            context.write(k2c,this.v2c);

        }
    }



    //Reduce阶段

    static  class CalAvgTempUseCombiner1Reducer
            extends  Reducer<Text,AvgNum1,Text,DoubleWritable>{


        private  Text k3=new Text();
        private DoubleWritable v3=new DoubleWritable();


        @Override
        protected void reduce(Text k2, Iterable<AvgNum1> v2s, Context context) throws IOException, InterruptedException {


            //1.计算总的个数
            long count=0;
            //2.计算平均值
            //先求总和，在求平均值
            double sum=0;

            for (AvgNum1 v2 : v2s) {
                sum+=(v2.getAvg().get()*v2.getNum().get());
                count+=v2.getNum().get();
            }

            double avg=sum/count;

            //3.将k2和平均值交给框架，作为最终结果

            this.k3.set(k2.toString());
            this.v3.set(avg);

            //提交
            context.write(this.k3,this.v3);

        }
    }





    @Override
    public int run(String[] strings) throws Exception {

        //获取conf对象
        Configuration conf = this.getConf();
        //设置输入输出路径
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        //获取job对象
        Job job = Job.getInstance(conf, "计算Combiner优化计算平均值");
        job.setJarByClass(this.getClass());


        //配置map阶段
        job.setMapperClass(CalAvgTempUseCombiner1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgNum1.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);       //add可以添加多个输入路径


        //Combiner
        job.setCombinerClass(CalAvgTempUseCombiner1Combiner.class);


        //配置reduce阶段
        job.setReducerClass(CalAvgTempUseCombiner1Reducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);    //set只能设置一个输出路径


        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CalAvgTempUseCombiner1(),args));
    }
}
