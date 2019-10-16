package mapReduce.db;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: bd1903.hadoop
 * @package: com.briup.bigdata.bd1903.hadoop.mr.db
 * @filename: HDFS2DB.java
 * @create: 2019.10.08 17:06
 * @author: Kevin
 * @description: .
 **/
public class HDFS2DB extends Configured implements Tool{


    static class HDFS2DBMapper extends Mapper<LongWritable,Text,YearSidTemp,NullWritable>{
        private YearSidTemp k2=new YearSidTemp();
        private NullWritable v2=NullWritable.get();

        @Override
        protected void map(LongWritable k1,Text v1,Context context) throws IOException, InterruptedException{
            String[] strs=v1.toString().split("[\t]");

            this.k2.setYear(Integer.parseInt(strs[0]));
            this.k2.setSid(strs[1]);
            this.k2.setTemp(Double.parseDouble(strs[2]));

            context.write(this.k2,this.v2);
        }
    }


    @Override
    public int run(String[] args) throws Exception{
        Configuration conf=this.getConf();
        Path in=new Path(conf.get("in"));

        Job job=Job.getInstance(conf,"HDFS2DB");
        job.setJarByClass(this.getClass());

        job.setMapperClass(HDFS2DBMapper.class);
        job.setMapOutputKeyClass(YearSidTemp.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job,in);

        job.setOutputKeyClass(YearSidTemp.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        // 配置数据的连接信息
        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://192.168.136.134:3306/bd1903",
                "root",
                "362326");

        // 配置数据输出的信息
        DBOutputFormat.setOutput(job,"tbl_yst","year","sid","temp");

        return job.waitForCompletion(true)?0:1;
    }


    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new HDFS2DB(),args));
    }

}
