package com.hadooptraining.lab8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/************************************************************************
 *                                LAB 8                                 *
 ************************************************************************/

/**
 * Hadoop enables us to implement and specify custom InputFormat implementations
 * for our MapReduce computations. We can implement custom InputFormat
 * implementations to gain more control over the input data as well as to support
 * proprietary or application-specific input data file formats as inputs to
 * Hadoop MapReduce computations. A InputFormat implementation should extend
 * the org.apache.hadoop.mapreduce.InputFormat<K,V> abstract class overriding
 * the createRecordReader() and getSplits() methods. In this lab, we implement
 * a InputFormat and a RecordReader for the HTTP log files. This InputFormat
 * will generate LongWritable instances as keys and LogWritable instances as
 * the values.
 */
public class LogProcessorWithCustomInputFormat extends Configured implements Tool {

    /**
     * The run method for constructing your job. This is required according to the Tools interface.
     * The Tools interface helps constructing a Hadoop job that needs reading, parsing, and
     * processing command-line arguments.
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {

        // If the number of arguments is insufficient, print an error message and exit
        if (args.length < 3) {
            System.err.println("Usage:  <input_path> <output_path> <num_reduce_tasks>");
            System.exit(-1);
        }

        // Your job is handled by the Job object - managed by the JobTracker
        Job job = Job.getInstance(getConf(), "log-analysis");

        // This locates the jar file that needs to be run by using a class name
        job.setJarByClass(LogProcessorWithCustomInputFormat.class);

        // Set the mapper class
        job.setMapperClass(LogProcessorMap.class);

        // Set the reducer class
        job.setReducerClass(LogProcessorReduce.class);

        // Set the reducer output key class
        job.setOutputKeyClass(Text.class);

        // Set the reducer output value class
        job.setOutputValueClass(IntWritable.class);

        // TODO STUDENT

        // Add the input and output paths from program arguments
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Get the number of reduce tasks from the third argument
        int numReduce = Integer.parseInt(args[2]);

        // Set the number of reduce tasks
        job.setNumReduceTasks(numReduce);

        // Run the job and store the result
        boolean jobResult = job.waitForCompletion(true);

        // Find the counters from the Job object
        Counters counters = job.getCounters();

        // Retrieve the bad records and print it

        // TODO STUDENT

        // Return the status depending on the success of the job
        return jobResult ? 0 : 1;
    }

    /**
     * This is the main program, which just calls the ToolRunner's run method.
     * @param args arguments to the program
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Invoke the ToolRunner's run method with required arguments
        int res = ToolRunner.run(new Configuration(), new LogProcessorWithCustomInputFormat(), args);

        // Return the same exit code that was returned by ToolRunner.run()
        System.exit(res);
    }
}