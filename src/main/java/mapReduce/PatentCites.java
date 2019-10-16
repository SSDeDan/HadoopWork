package mapReduce;

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
 * @Created 2019/9/26 16:34
 *
 *      使用MapReduce程序计算专利引用；
 *      计算每个专利都引用了那些专利
 */
public class PatentCites extends Configured implements Tool {


    //1.编写Map任务
    static class PatentCitesMapper
            extends Mapper<LongWritable, Text,Text,Text>{

        private  Text k2=new Text();
        private  Text v2=new Text();


        @Override
        protected void map(
                LongWritable k1,       //读取到的一行数据的偏移量，k1
                Text v1,             //读取到的一行数据的，v1
                Context context         //联系Map任务和MR框架的对象
        ) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("[,]");

            this.k2.set(strs[0]);

            this.v2.set(strs[1]);

            System.out.println("Map阶段： "+this.k2+"="+this.v2);

            context.write(this.k2,this.v2);

        }
    }


    //2.编写Reduce任务

    static class PatentCitesReducer
            extends Reducer<Text,Text,Text,Text>{

        private  Text k3=new Text();
        private  Text v3=new Text();


        @Override
        protected void reduce(
                Text k2,               //k2
                Iterable<Text> v2s,  //v2
                Context context         //联系Reduce和MR框架的对象

        ) throws IOException, InterruptedException {

            StringBuffer sb=new StringBuffer();

            v2s.forEach(v2->  sb.append(v2.toString()).append(","));

            this.k3.set(k2.toString());
            this.v3.set(sb.substring(0,sb.length()-1));

            System.out.println("Reduce阶段： "+this.k3+"="+this.v3);

            context.write(this.k3,this.v3);
        }
    }

    // 3.作业配置，作业配置都是配置在run方法中；
    // 在整个MR程序运行的时候，run方法中的代码会在客户端执行；
    // 编写的Map任务和Reduce任务会在集群上执行；
    // 所以在run方法中出现的打印语句可以打印到控制台上，而在
    // Map或者Reduce任务中出现的打印语句不会被打印到控制台上，
    // 而是以日志的形式被记录日志文件中；
    @Override
    public int run(String[] strings) throws Exception{
        // 将程序打包至客户端【集群中的任意一个节点】上运行的时候
        // 下面代码会根据该客户端配置自动读取配置文件
        Configuration conf=this.getConf();
        // 如果输入路径指定的是一个文件，则MR程序只处理该文件
        // 如果输入路径指定的是一个目录，则MR程序会处理该目录下
        // 的所有文件；对于子目录无效；
        Path in=new Path(conf.get("in"));
        // 关于MapReduce程序运行时的结果存放路径在HDFS集群上一定不能
        // 预先存在，如果存在则该程序运行报异常；
        // MR程序的运行结果会被存放在指定的目录中；
        // MR程序处理数据的结果会生成多个文件：
        //      _SUCCESS
        //      part-r-00000
        //      part-r-00001
        // Map任务的个数：和数据分片相关，也就是和数据块有关；
        // Reduce任务的个数：默认是1个；Reduce的个数是可以配置的；
        //                  有多少个Reduce，就有多少个结果文件，
        //                  不一定所有的Reduce都会参数数据的计算；
        //                  Reduce处理的结果文件中，有些文件有可能
        //                  是空的；
        // 命令行>代码中的配置>配置文件中的配置>默认
        Path out=new Path(conf.get("out"));

        Job job=Job.getInstance(conf,"专利引用");
        job.setJarByClass(this.getClass());

        // Map任务的配置
        // 配置Map任务要运行的类
        job.setMapperClass(PatentCitesMapper.class);
        // 配置Map任务输出的k2和v2的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 配置Map端读取原始数据的格式，
        // 配置Map任务以什么样的方式读取原始数据
        // 以下配置决定了数据进入Map端的k1和v1的数据类型
        // KeyValueInputFormat
        job.setInputFormatClass(TextInputFormat.class);
        // 配置文件的输入路径
        TextInputFormat.addInputPath(job,in);

        // 配置Reduce任务
        job.setReducerClass(PatentCitesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        System.out.println("任务配置已经完成！");

        // 设置Reduce的个数
//        job.setNumReduceTasks(5);

        // 设置Combiner类，
        job.setCombinerClass(PatentCitesReducer.class);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new PatentCites(),args));

    }



}
