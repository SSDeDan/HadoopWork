package homeWork;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/26 14:23
 *
 *
 *          每个专利被引用的次数
 *
 *
 */
public class PatentCount {
    public static void main(String[] args) throws IOException {
        //获取Configuration对象，配置集群信息
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://ud1:9000");

//        conf.set("hadoop.home.dir", "D:/Developer/hadoop-2.8.4");

        ////获取文件系统对象0

        FileSystem fs = FileSystem.get(conf);

        //从集群里面读取文件数据
        FSDataInputStream fsdis = fs.open(new Path("/data/patent/cite75_99.txt"));

        BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));

        HashMap<String,Integer> map = new HashMap<>();
        String line;
        while ((line=br.readLine())!=null){

            String[] strs = line.split("[,]");
            map.put(strs[1],map.get(strs[1])!=null?(map.get(strs[1])+1):1);

        }


        FSDataOutputStream fsdos = fs.create(new Path("/pcount.txt"));
        PrintWriter pw = new PrintWriter(fsdos, true);



        map.forEach((k,v)-> pw.println(k+":::"+v));




    }

}
