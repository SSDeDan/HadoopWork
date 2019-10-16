package File;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/26 15:15
 */
public class SeqFileReader {

    public static void main(String[] args) throws IOException {


        Configuration conf = new Configuration();

        SequenceFile.Reader.Option o1 =
                SequenceFile.Reader.file(new Path("/bj.seq"));

        SequenceFile.Reader reader = new SequenceFile.Reader(conf,o1);

        Text key = new Text();
       Text value = new Text();




//        Configuration conf = new Configuration();
//
//        SequenceFile.Reader.Option o1 = SequenceFile.Reader.file(new Path("/bj.seq"));
//
//
//        SequenceFile.Reader reader = new SequenceFile.Reader(conf, o1);
//
//        Text key = new Text();
//        Text value = new Text();
//
//        //1.判断该序列文件中有没有下一个能够读取的Record；
//        //2.如果有能够读取的record，则读取该record并将其key和value分别赋值给参数中的key和value；
//        while (reader.next(key,value)){
//
//            System.out.println(key+"::::"+value);
//
//
//        }


    }
}
