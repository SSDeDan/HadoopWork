package mapReduce.reduce_join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @package: mapReduce.reduce_join
 * @filename: KeyFlag.java
 * @create: 2019.10.08 16:02
 * @description: .
 **/
public class KeyFlag implements WritableComparable<KeyFlag>{
    private Text key;
    private Text flag;

    public KeyFlag(){
        this.key=new Text();
        this.flag=new Text();
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof KeyFlag)) return false;
        KeyFlag keyFlag=(KeyFlag)o;
        return key.equals(keyFlag.key)&&flag.equals(keyFlag.flag);
    }

    @Override
    public int hashCode(){
        return Objects.hash(key,flag);
    }

    @Override
    public int compareTo(KeyFlag o){
        int keyComp=this.key.compareTo(o.key);
        int flagComp=this.flag.compareTo(o.flag);
        return keyComp==0?flagComp:keyComp;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        this.key.write(out);
        this.flag.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        this.key.readFields(in);
        this.flag.readFields(in);
    }

    public Text getKey(){
        return key;
    }

    public void setKey(Text key){
        this.key.set(key.toString());
    }

    public Text getFlag(){
        return flag;
    }

    public void setFlag(String flag){
        this.flag.set(flag);
    }

    @Override
    public String toString(){
        return "KeyFlag{"+"key="+key+", flag="+flag+'}';
    }
}
