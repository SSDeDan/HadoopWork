package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/25 14:12
 */
public class GetFile{
    public static void main(String[] args) throws IOException{
        Configuration conf=new Configuration();
        // conf.set("fs.defaultFS","ftp://ud3:9000");
        // core-site.xml
        // fs.defaultFS     hdfs://ud3:9000
        // fs.defaultFS     file:///
        FileSystem fs=FileSystem.get(conf);
        System.out.println(fs.getClass().getName());
        // DistributedFileSystem
        // HttpFileSystem
        // FTPFileSystem
        // LocalFileSystem
        Path rf=new Path(args[0]);
        FSDataInputStream fsdis=fs.open(rf);
        // DFSInputStream



        FileOutputStream fos=
                new FileOutputStream(args[1]);
        IOUtils.copyBytes(fsdis,fos,1024,true);
    }
}

