package mapReduce.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;





public class YearSidTemp implements DBWritable, WritableComparable<YearSidTemp>{
    private IntWritable year;
    private Text sid;
    private DoubleWritable temp;

    public YearSidTemp(){
        this.year=new IntWritable();
        this.sid=new Text();
        this.temp=new DoubleWritable();
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException{
        // insert into bd1903.tbl_yst(year,sid,temp)
        // values(?,?,?);
        ps.setInt(1,this.year.get());
        ps.setString(2,this.sid.toString());
        ps.setDouble(3,this.temp.get());
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException{
        // select year,sid,temp from bd1903.tbl_yst;
        this.year.set(rs.getInt(1));
        this.sid.set(rs.getString(2));
        this.temp.set(rs.getDouble(3));
    }

    @Override
    public int hashCode(){
        return Objects.hash(year,sid,temp);
    }


    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof YearSidTemp)) return false;
        YearSidTemp that=(YearSidTemp)o;
        return year.equals(that.year)&&sid.equals(that.sid)&&temp.equals(that.temp);
    }

    @Override
    public String toString(){
        return this.year+"\t"+this.sid+"\t"+this.temp;
    }

    @Override
    public int compareTo(YearSidTemp o){
        int yc=this.year.compareTo(o.year);
        int sc=this.sid.compareTo(o.sid);
        int tc=this.temp.compareTo(o.temp);
        return yc==0?(sc==0?tc:sc):yc;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        this.year.write(out);
        this.sid.write(out);
        this.temp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        this.year.readFields(in);
        this.sid.readFields(in);
        this.temp.readFields(in);
    }

    public IntWritable getYear(){
        return year;
    }

    public void setYear(IntWritable year){
        this.year.set(year.get());
    }

    public void setYear(int year){
        this.year.set(year);
    }

    public Text getSid(){
        return sid;
    }

    public void setSid(String sid){
        this.sid.set(sid);
    }

    public void setSid(Text sid){
        this.sid.set(sid.toString());
    }

    public DoubleWritable getTemp(){
        return temp;
    }

    public void setTemp(double temp){
        this.temp.set(temp);
    }

    public void setTemp(DoubleWritable temp){
        this.temp.set(temp.get());
    }
}
