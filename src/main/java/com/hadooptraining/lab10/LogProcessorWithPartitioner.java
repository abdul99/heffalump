package com.hadooptraining.lab10;

import com.hadooptraining.lab8.LogFileInputFormat;
import com.hadooptraining.lab8.LogProcessorMap;
import com.hadooptraining.lab8.LogProcessorReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop partitions the intermediate data generated from the Map tasks across the reduce tasks
 * of the computations. A proper partitioning function ensuring balanced load for each reduce
 * task is crucial to the performance of MapReduce computations. Partitioning can also be used
 * to group together related set of records to specific reduce tasks, where you want the
 * certain outputs to be processed or grouped together. Hadoop partitions the intermediate
 * data based on the key space of the intermediate data and decides which reduce task will
 * receive which intermediate record. The sorted set of keys and their values of a partition
 * would be the input for a reduce task. In Hadoop, the total number of partitions should
 * be equal to the number of reduce tasks for the MapReduce computation. Hadoop Partitioners
 * should extend the org.apache.hadoop.mapreduce.Partitioner<KEY,VALUE> abstract class.
 * Hadoop uses org.apache.hadoop.mapreduce.lib.partition.HashPartitioner as the default
 * Partitioner for the MapReduce computations. HashPartitioner partitions the keys based
 * on their hashcode(), using the formula key.hashcode() mod r, where r is the number of
 * reduce tasks. There can be scenarios where our computations logic would require or
 * can be better implemented using an application's specific data-partitioning schema.
 * In this recipe, we implement a custom Partitioner for our HTTP log processing application,
 * which partitions the keys (IP addresses) based on their geographic regions.
 *
 * In this lab we'll introduce a simple partitioner for the same log processing application
 * we wrote in Lab 8. The partitioner will use the IP to determine the reducer id.
 */
public class LogProcessorWithPartitioner extends Configured implements Tool {

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
            System.err.println("Usage: <input_path> <output_path> <num_reduce_tasks>");
            System.exit(-1);
        }

        // Your job is handled by the Job object - managed by the JobTracker
        Job job = Job.getInstance(getConf(), "log-analysis");

        // This locates the jar file that needs to be run by using a class name
        job.setJarByClass(LogProcessorWithPartitioner.class);

        // Set the mapper and reducer classes
        job.setMapperClass(LogProcessorMap.class);
        job.setReducerClass(LogProcessorReduce.class);

        // Set reducer output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the InputFormat class
        job.setInputFormatClass(LogFileInputFormat.class);

        // Configure the partitioner
        job.setPartitionerClass(IPBasedPartitioner.class);

        // Add the input and output paths from program arguments
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Extract the number of reduce tasks from one of the arguments
        int numReduce = Integer.parseInt(args[2]);

        // Set the number of reduce tasks
        job.setNumReduceTasks(numReduce);

        // Fire the job and return job status based on success of job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * This is the main program, which just calls the ToolRunner's run method.
     * @param args arguments to the program
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Invoke the ToolRunner's run method with required arguments
        int res = ToolRunner.run(new Configuration(), new LogProcessorWithPartitioner(), args);

        // Return the same exit code that was returned by ToolRunner.run()
        System.exit(res);
    }

}