package com.atguigu.ch06.combiner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/3
 */
public class WordProducer {
    private static final String[] words = new String[]{"Jaunary", "Feburary", "March", "April",
    "May", "June", "July", "August", "Septemble", "October", "Nov", "Dec"};

    public static void main(String[] args) throws IOException {
        FileOutputStream os = new FileOutputStream(new File("E:\\input2\\month.txt"));
        for(int i = 0; i <= 10000000; i++) {
            String word = words[(int) (Math.random() * words.length)] + " ";
            os.write(word.getBytes());
            if(i % 10 == 0) {
                os.write('\n');
            }
        }

        os.close();
    }
}
