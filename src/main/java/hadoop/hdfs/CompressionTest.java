package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/25 14:29
 */
public class CompressionTest{
    public static void main(String[] args) throws IOException{
        // testCompression();
        testDecompression();
    }

    public static void testCompression() throws IOException{
        Configuration conf=new Configuration();

        conf.set("fs.defaultFS","hdfs://ud1:9000");

        // 数据块的大小是由客户端指定的
        // 以下两项配置是在客户端生效
        conf.set("dfs.blocksize","32M");
        conf.set("dfs.replication","1");

        FileSystem fs=FileSystem.get(conf);

        // 上传文件需要压缩，需要编码器
        // 1.CompressionCodecFactory通过工厂模式会根据上传至集群的
        // 文件的扩展名自动获取相应的编码器；
        // 2.在代码中直接通过new创建相应的编码器对象；
        String filePath="/c.gz";

        // 正常的输出流对象
        FSDataOutputStream fsdos=fs.create(new Path(filePath));

        // 要对数据进行压缩，则需要使用编码器对输出流进行压缩
        //获取codec对象的方式一

         CompressionCodecFactory factory=
             new CompressionCodecFactory(conf);

         CompressionCodec codec=
             factory.getCodec(new Path(filePath));

         //获取codec对象的方式二

//        GzipCodec codec=new GzipCodec();
//
//        codec.setConf(conf);

        CompressionOutputStream cos=
                codec.createOutputStream(fsdos);

        FileInputStream fis=
                new FileInputStream("D:\\Documents\\Desktops\\笔记.txt");

        IOUtils.copyBytes(fis,cos,1024,true);
    }

    public static void testDecompression() throws IOException{
        Configuration conf=new Configuration();

        conf.set("fs.defaultFS","hdfs://ud1:9000");

        FileSystem fs=FileSystem.get(conf);

        Path path=new Path("/c.gz");

        FSDataInputStream fsdis=fs.open(path);

        // CompressionCodecFactory factory=new CompressionCodecFactory(conf);
        //
        // CompressionCodec codec=factory.getCodec(path);

        GzipCodec codec=new GzipCodec();

        codec.setConf(conf);

        CompressionInputStream cis=codec.createInputStream(fsdis);

        BufferedReader br=new BufferedReader(new InputStreamReader(cis));

        String line;

        while((line=br.readLine())!=null){
            System.out.println(line);
        }

    }
}
