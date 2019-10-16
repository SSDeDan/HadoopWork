package File;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @program: shuaixiaohuo.java
 * @Description: HadoopWork
 * @author: ZZZss
 * @Created 2019/9/27 9:23
 */
public class createSmallFiles {
    public static void main(String[] args) throws IOException {

        File dir = new File("D:\\smallfiles");
        if (!dir.exists()) dir.mkdirs();

        for (int x=0;x<10000;x++){
            File file = new File(dir, x + ".txt");
            PrintWriter pw = new PrintWriter(new FileWriter(file), true);
            pw.println(x);
            pw.close();

        }



    }
}
