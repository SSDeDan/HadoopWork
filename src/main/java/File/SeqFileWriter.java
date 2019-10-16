package File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/26 14:48
 *
 *
 *      生成序列文件，存储10000个小文件；
 *      key是文件的文件绝对路径，value是文件的内容
 *
 */
public class SeqFileWriter {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        //createWriter方法需要以下参数
        //1.Configuration对象
        //2.类型为SequenceFile.Writer.Option的可变长参数列表在生成序列文件时，
        // 必须使用Option对象来表示
        //      1.生成的文件路径，
        //      2.key的数据类型，
        //      3.value的数据类型
        //      4.表示是否进行输出，以及压缩的级别

        //获取Writer对象需要传入以下四个参数
        SequenceFile.Writer.Option o1 =
                SequenceFile.Writer.file(new Path("/bj.seq"));

        SequenceFile.Writer.Option o2 =
                SequenceFile.Writer.keyClass(Text.class);

        SequenceFile.Writer.Option o3 =
                SequenceFile.Writer.valueClass(Text.class);

//        SequenceFile.Writer.Option o4 =
//                SequenceFile.Writer.compression(
//                        SequenceFile.CompressionType.RECORD,
//                        new Lz4Codec());

        //获得Writer对象
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,o1,o2,o3 );


        //从文件中读取内容获取key和value
        File dir = new File("D:\\笔记.txt");
        File[] files = dir.listFiles();
        if (files==null) return;
        for (File file:files){

            //获取key值
           Text key = new Text(file.getAbsolutePath());


           //获取value值
            BufferedReader br = new BufferedReader(new FileReader(file));

            String line;

            StringBuffer sb = new StringBuffer();

            while ((line=br.readLine())!=null){

            sb.append(line).append(System.getProperty("line.separator"));

            }
            Text value = new Text(sb.toString());


            //将key和value的值传入，
            // 通过writer.append将数据写入到SequenceFile文件中
            writer.append(key,value);
            br.close();
        }

        writer.close();



    }


}
