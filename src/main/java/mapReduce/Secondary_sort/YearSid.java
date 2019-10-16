package mapReduce.Secondary_sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 *
 *      自定义数据类型
 *
 * @author: ZZZss
 * @Created 2019/9/30 9:36
 */
public class YearSid implements WritableComparable<YearSid> {
    private IntWritable year;
    private  Text sid;

    public YearSid() {
        this.year=new IntWritable();
        this.sid=new Text();
    }


    @Override
    public int hashCode() {
        return Math.abs(this.year.hashCode()+this.sid.hashCode()*127);
    }

    @Override
    public boolean equals(Object obj) {
      if (this==obj)return  true;
      if (!(obj instanceof YearSid)) return  false;

      YearSid that= (YearSid) obj;

      return    this.year.equals(that.year)&&this.sid.equals(that.sid);

    }

    @Override
    public int compareTo(YearSid o) {
        int yearComp = this.year.compareTo(o.year);
        int sidComp = this.sid.compareTo(o.sid);

        return yearComp==0?sidComp:yearComp;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.year.write(dataOutput);
        this.sid.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year.readFields(dataInput);
        this.sid.readFields(dataInput);
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year.set(year);
    }

    public void setYear(IntWritable year) {
        this.year.set(year.get());
    }

    public Text getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid.set(sid);
    }

    public void setSid(Text sid) {
        this.sid.set(sid.toString());
    }

    @Override
    public String toString() {
        return this.year.get()+"\t"+this.sid.toString();
    }
}
