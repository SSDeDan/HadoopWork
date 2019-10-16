package mapReduce;

import org.apache.hadoop.io.Text;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/29 9:19
 */
public class RawWeatherDataParser{
    private int year;
    private String sid;
    private double temp;

    public boolean parse(Text line){
        return this.parse(line.toString());
    }

    public boolean parse(String line){
        // 数据长度不足
        if(line.length()<93) return false;
        // 如果采集到的数据是+9999，不符合条件
        if("+9999".equals(line.substring(87,92))) return false;
        // 数据质量不佳，不符合条件
        if(!"01459".contains(line.substring(92,93))) return false;


        this.year= Integer.parseInt(line.substring(15,19));
        this.sid=line.substring(0,15);
        this.temp=Double.parseDouble(line.substring(87,92));
        return true;
    }

    public int getYear(){
        return year;
    }

    public String getSid(){
        return sid;
    }

    public double getTemp(){
        return temp;
    }
}
