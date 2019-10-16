package mapReduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/29 14:10
 *
 *     自定义的数据类型 用于表示平均值和产生平均值的个数
 *
 */
public class AvgNum1 implements Writable {

    //平均值
    private DoubleWritable avg;
    //产生平均值的个数
    private LongWritable num;

    //在构造器里面初始化
    public AvgNum1() {
        this.avg=new DoubleWritable();
        this.num=new LongWritable();



    }


    //Getter和Setter方法

    public DoubleWritable getAvg() {
        return avg;
    }


    public void setAvg(double avg){
        this.avg.set(avg);
    }


    public void setAvg(DoubleWritable avg) {
        //值赋值/值复制的方式给成员变量赋值
        //取出avg中的值赋值给this.avg,就不需要将avg指向一个对象
        this.avg.set(avg.get());
    }

    public LongWritable getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num.set(num);
    }

    public void setNum(LongWritable num) {
        this.num.set(num.get());
    }



    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {


        this.avg.write(dataOutput);
        this.num.write(dataOutput);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {

    this.avg.readFields(dataInput);
    this.num.readFields(dataInput);


    }
}
