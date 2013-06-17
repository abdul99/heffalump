package com.hadooptraining.lab8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reducer for the job. It takes <K2,V2> as <Text, IntWritable> and emits <K3,V3> as <Text,IntWritable>.
 */
public class LogProcessorReduce extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    // Create a common IntWritable object to hold the result
    private IntWritable result = new IntWritable();

    /**
     * The reducer looks at the incoming <K,V> pair which happens to the IP address and the number of
     * bytes associated with it. All the reducer needs to do is sum up all the values (bytes) associates with
     * the key and write the sum to the context object.
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // TODO STUDENT

    }
}
