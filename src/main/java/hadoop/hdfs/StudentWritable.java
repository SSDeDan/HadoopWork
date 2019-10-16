package hadoop.hdfs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/25 16:14
 *
 *
 *          自定义Hadoop中的数据类型
 *
 *      1.除了重写Comparable接口中的compareTo方法还要重写Object类中的equals方法和hashCode方法，
 *      equals方法和hashCode方法在重写时只需要根据compareTo方法中用到的字段【成员变量】即可
 *
 *
 */
public class StudentWritable
        implements WritableComparable<StudentWritable>{
    private IntWritable id;
    private Text name;
    private double score;
    private List<Text> courses;

    public StudentWritable(){
        this.id=new IntWritable();
        this.name=new Text();
        this.courses=new ArrayList<>();
    }

    @Override
    public int hashCode(){
        return this.id.hashCode();
    }

    @Override
    public boolean equals(Object obj){
        if(this==obj) return true;
        if(!(obj instanceof StudentWritable)) return false;
        StudentWritable sw=(StudentWritable)obj;
        return this.id.equals(sw.id);
    }

    @Override
    public int compareTo(StudentWritable o){
        return this.id.compareTo(o.id);
        // int idComp=this.id.compareTo(o.id);
        // int nameComp=this.name.compareTo(o.name);
        // return idComp==0?nameComp:idComp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        this.id.write(dataOutput);
        this.name.write(dataOutput);


        new DoubleWritable(this.score).write(dataOutput);

        new IntWritable(this.courses.size()).write(dataOutput);

        for(Text course: this.courses){
            course.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        // 先序列化谁要先反序列化谁
        this.id.readFields(dataInput);
        this.name.readFields(dataInput);


        DoubleWritable dw = new DoubleWritable();
        dw.readFields(dataInput);
        this.score=dw.get();





        IntWritable size=new IntWritable();
        size.readFields(dataInput);

        this.courses.clear();

        // List<Text> list=new ArrayList<>();

        for(int x=0;x<size.get();x++){
            Text text=new Text();
            text.readFields(dataInput);
            this.courses.add(text);
        }
    }

    public IntWritable getId(){
        return id;
    }

    public void setId(IntWritable id){
        this.id=id;
    }

    public Text getName(){
        return name;
    }

    public void setName(Text name){
        this.name=name;
    }

    public double getScore(){
        return score;
    }

    public void setScore(double score){
        this.score=score;
    }

    public List<Text> getCourses(){
        return courses;
    }

    public void setCourses(List<Text> courses){
        this.courses=courses;
    }

    @Override
    public String toString(){
        return "StudentWritable{"+"id="+id+", name="+name+", score="+score+", courses="+courses+'}';
    }
}