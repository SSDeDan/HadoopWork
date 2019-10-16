package hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/24 16:59
 */
public class PutFile {
    public static void main(String[] args) throws IOException {

        //获取Configuration对象，配置集群信息
        Configuration conf = new Configuration();

        //C:\Windows\System32\drivers\etc\hosts
        conf.set("fs.defaultFS","hdfs://ud1:9000");

        //获取文件系统对象
        FileSystem fs = FileSystem.get(conf);


        //创建文件
        Path file = new Path("/grms/rawdata/data.txt");
        FSDataOutputStream fsdos = fs.create(file);

        FileInputStream fis =
                new FileInputStream("D:\\大数据\\培训\\课程\\商品推荐\\grms-day1\\data.txt");

        //边读边写
        IOUtils.copyBytes(fis,fsdos,1024,true);

        // winutils.exe
        // 1.在windows中安装hadoop并配置环境变量
        // 2.将文件winutils.exe拷贝至hadoop安装目录的bin目录下

    }

}
