package com.hadooptraining.lab8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class that takes <K1,V1> pair as <Object, LogWritable> and writes
 * a <K2,V2> pair of <Text, IntWritable>.
 */
public class LogProcessorMap extends Mapper<Object, LogWritable, Text, IntWritable> {

    /**
     * Define a custom counter to count the number of bad or corrupted records in our log processing application.
     */
    public static enum LOG_PROCESSOR_COUNTER {
        BAD_RECORDS
    }

    /**
     * The mapper that takes <K1,V1> as <Object, LogWritable>. This function gets a LogWritable object
     * directly, and writes the key-value pair to the context object. The key is simply the user's
     * IP address and the value is the response size in bytes as found in the LogWritable object.
     * @param key the incoming key
     * @param value the incoming value
     * @param context the context object
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, LogWritable value, Context context)
            throws IOException, InterruptedException {

        // If the response size is less than 1, we consider it a bad record
        if (value.getResponseSize().get() < 1) {
            context.getCounter(LOG_PROCESSOR_COUNTER.BAD_RECORDS).increment(1);
        }

        // Send the key and value to the context object
        context.write(value.getUserIP(), value.getResponseSize());
    }
}
