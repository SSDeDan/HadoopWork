package mapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/10/8 14:06
 */
public class CalcSocre2 extends Configured implements Tool {





























    @Override
    public int run(String[] strings) throws Exception {

//        //获取conf对象
//        Configuration conf = this.getConf();
//        //设置输入输出路径
//        Path in = new Path(conf.get("in"));
//        Path out = new Path(conf.get("out"));
//
//        //获取job对象
//        Job job = Job.getInstance(conf, "计算Combiner优化计算平均值");
//        job.setJarByClass(this.getClass());
//
//
//        //配置map阶段
//        job.setMapperClass(CalAvgTempUseCombiner1Mapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(AvgNum1.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job,in);       //add可以添加多个输入路径
//
//
//        //Combiner
//        job.setCombinerClass(CalAvgTempUseCombiner1Combiner.class);
//
//
//        //配置reduce阶段
//        job.setReducerClass(CalAvgTempUseCombiner1Reducer.class);
//        job.setOutputValueClass(Text.class);
//        job.setOutputValueClass(DoubleWritable.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job,out);    //set只能设置一个输出路径
//

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CalcSocre2(),args));
    }
}
