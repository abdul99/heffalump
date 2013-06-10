package com.hadooptraining.lab6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/************************************************************************
 *                                LAB 6                                 *
 ************************************************************************/

/**
 * The word count sample counts the number of word occurrences within a set of input documents
 * using MapReduce. The code has three parts: mapper, reducer, and the main program.
 *
 * Often Hadoop jobs are executed through a command line. Therefore, each Hadoop job has to support reading,
 * parsing, and processing command-line arguments. To avoid each developer having to rewrite this code,
 * Hadoop provides a org.apache.hadoop.util.Tool interface.
 *
 * This class is boiler-plate code to write your main() function using the Tool interface.
 */
public class WordCountWithTools extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        // If the number of arguments is insufficient, print an error message and exit
        if (args.length < 2) {
            System.out.println("Usage: WordCountWithTools <inDir> <outDir>");
            System.out.println("Example: WordCountWithTools input output");
            ToolRunner.printGenericCommandUsage(System.out);
            System.out.println("");
            return -1;
        }

        // Your job is handled by the Job object - managed by the JobTracker
        Job job = Job.getInstance(getConf(), "Word count with tools");

        // This locates the jar file that needs to be run by using a class name
        job.setJarByClass(WordCount.class);

        // Set the mapper class
        job.setMapperClass(WordCount.TokenizerMapper.class);

        // Set the reducer class
        job.setReducerClass(IntSumReducer.class);

        // Set the output key class
        job.setOutputKeyClass(Text.class);

        // Set the output value class
        job.setOutputValueClass(IntWritable.class);

        // Add the input and output paths from program arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
        int res = ToolRunner.run(new Configuration(), new WordCountWithTools(), args);

        // Return the same exit code that was returned by ToolRunner.run()
        System.exit(res);
    }
}
