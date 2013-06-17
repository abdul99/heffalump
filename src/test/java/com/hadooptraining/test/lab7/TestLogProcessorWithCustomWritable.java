package com.hadooptraining.test.lab7;

import com.hadooptraining.lab7.LogProcessorWithCustomWritable;
import com.hadooptraining.lab7.LogWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class TestLogProcessorWithCustomWritable {
    private MapDriver<LongWritable, Text, Text, LogWritable> mapDriver;
    private ReduceDriver<Text, LogWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setup() {
        LogProcessorWithCustomWritable.LogProcessorMap mapper = new LogProcessorWithCustomWritable.LogProcessorMap();
        LogProcessorWithCustomWritable.LogProcessorReduce reducer = new LogProcessorWithCustomWritable.LogProcessorReduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        // An example input field
        String line = "205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] \"GET /shuttle/countdown/countdown.html HTTP/1.0\" 200 3985";

        // This must match the input <K,V> template of the mapper. This is the input to the mapper.
        mapDriver.withInput(new LongWritable(0), new Text(line));

        // Generate the expected output key
        Text outKey = new Text("205.212.115.106");

        // Generate the expected output value
        LogWritable outVal = new LogWritable();

        // Now set the output value
        outVal.set("205.212.115.106", "01/Jul/1995:00:00:12 -0400", "GET /shuttle/countdown/countdown.html HTTP/1.0", 3985, 200);

        // Set the expected output <K,V> pair
        mapDriver.withOutput(outKey, outVal);

        // Now run the test to check if the given input results in the expected output
        // mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        // Create first input record for the reducer
        LogWritable log1 = new LogWritable();
        log1.set("205.212.115.106", "01/Jul/1995:00:00:12 -0400", "GET /shuttle/countdown/countdown.html HTTP/1.0", 3985, 200);

        // Create second input record for the reducer
        LogWritable log2 = new LogWritable();
        log2.set("205.212.115.106", "01/Jul/1995:00:01:13 -0400", "GET /shuttle/missions/sts-71/images/images.html HTTP/1.0", 7634, 200);

        // Create expected output record for the reducer
        IntWritable outCount = new IntWritable();
        outCount.set(3985 + 7634);

        // Create the key for the reducer
        Text outkey = new Text("205.212.115.106");

        // Set the input and output values for the reducer
        reduceDriver.withInput(outkey, Arrays.asList(log1, log2));
        reduceDriver.withOutput(outkey, outCount);

        // Now run the test
        // reduceDriver.runTest();
    }
}
