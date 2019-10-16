package hadoop.hdfs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/25 19:31
 */
public class StudentWritableTest{
    public static void main(String[] args) throws IOException {
        StudentWritable sw1=new StudentWritable();
        sw1.setId(new IntWritable(110));
        sw1.setName(new Text("张三"));
        sw1.setScore(99.9);

        List<Text> list=new ArrayList<>();
        list.add(new Text("语文"));
        list.add(new Text("数学"));
        list.add(new Text("英语"));
        list.add(new Text("自然"));
        list.add(new Text("社会"));

        sw1.setCourses(list);

        System.out.println(sw1);


        //序列化和反序列化

        DataOutputStream dos=
                new DataOutputStream(new FileOutputStream("F:\\swx.txt"));

        //序列化
        sw1.write(dos);

        DataInputStream dis=new
                DataInputStream(new FileInputStream("F:\\swx.txt"));

        StudentWritable sw2=new StudentWritable();

        //反序列化，将数据读到sw2中
        sw2.readFields(dis);

        //打印sw2
        System.out.println(sw2);

    }
}
