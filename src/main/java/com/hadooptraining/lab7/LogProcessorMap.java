package com.hadooptraining.lab7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogProcessorMap extends Mapper<Object, LogWritable, Text, IntWritable > {
    public void map(Object key, LogWritable value, Context context)
            throws IOException, InterruptedException {

        // make bytes longWritable and output two value types...
        context.write(value.getUserIP(),value.getResponseSize());
    }
}
