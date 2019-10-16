package homeWork;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/26 9:18
 *
 *
 *         处理： 从集群里面读取文件，并将处理结果放回到集群里面
 *
 *        //计算每个专利被引用的次数；
 *
 *         //计算每个专利都引用了哪些专利；
 *
 *
 *
 */
public class zhuanliData {
    public static void main(String[] args) throws IOException {

        //计算每个专利被引用的次数；
        sum();
        //计算每个专利都引用了哪些专利；
        which();
    }



    //计算1.计算每个专利被引用的次数；  cite75_99.txt
    public   static void  sum() throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //从文件系统读取数据
        FSDataInputStream fsdis = fs.open(new Path("/data/patent/cite75_99.txt"));

        BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));

        HashMap<String,Integer> map = new HashMap<>();
        String line;
        while ((line=br.readLine())!=null){
            String[] strs = line.split("[,]");
            map.put(strs[1],map.get(strs[1])!=null?map.get(strs[1])+1:1);
        }
        FSDataOutputStream fsdos = fs.create(new Path("/data/patent/1.txt"));
        PrintWriter pw = new PrintWriter(fsdos, true);

        map.forEach((k,v)->pw.println(k+":::    ["+v+"]"));
    }


    //2.计算每个专利都引用了哪些专利    cite75_99.txt
    public  static  void  which() throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //从文件系统读取数据
        FSDataInputStream fsdis = fs.open(new Path("/data/patent/cite75_99.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));
        HashMap<String,String> map = new HashMap<>();
        String line;
        while ((line=br.readLine())!=null){
            String[] strs = line.split("[,]");
            map.put(strs[0],map.get(strs[0])!=null?map.get(strs[0])+","+strs[1]:strs[1]);
        }
        FSDataOutputStream fsdos = fs.create(new Path("/data/patent/2.txt"));
        PrintWriter pw = new PrintWriter(fsdos, true);

        map.forEach((k,v)->pw.println(k+":::   ["+v+"]"));

    }
}
