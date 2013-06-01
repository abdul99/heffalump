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
        if (args.length < 2) {
            System.out.println("Usage: WordCountWithTools <inDir> <outDir>");
            System.out.println("Example: WordCountWithTools input output");
            ToolRunner.printGenericCommandUsage(System.out);
            System.out.println("");
            return -1;
        }

        Job job = new Job(getConf(), "Word count with tools");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountWithTools(), args);
        System.exit(res);
    }
}
