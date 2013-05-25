package com.experimental.fileutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSJavaAPIDemo {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        System.out.println(fs.getUri());

        Path file = new Path("demo.txt");
        if (fs.exists(file)) {
            System.out.println("File already exists");
        } else {
            FSDataOutputStream outputStream = fs.create(file);
            outputStream.writeUTF("Welcome to HDFS Java API!!!");
            outputStream.close();
        }

        // Reading from file
        FSDataInputStream inputStream = fs.open(file);
        String data = inputStream.readUTF();
        System.out.println(data);
        inputStream.close();

        fs.close();
    }
}
