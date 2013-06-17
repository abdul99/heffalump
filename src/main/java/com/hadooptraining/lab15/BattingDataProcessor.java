package com.hadooptraining.lab15;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/************************************************************************
 *                                LAB 15                                *
 ************************************************************************/
/**
 * This class calculates the runs made in any given year since baseball records are available.
 * We use the year as the key for the MapReduce job. The value is a custom writable object called
 * BattingWritable.
 */
public class BattingDataProcessor extends Configured implements Tool {

    /**
     * Mapper class using 'year' as key and 'BattingWritable' as VALUE.
     */
    public static class BattingDataProcessorMap extends
            Mapper<LongWritable, Text, Text, BattingWritable> {

        private Text year = new Text();
        private BattingWritable battingValue = new BattingWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Format: playerID,yearID,stint,teamID,lgID,G,G_batting,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP,G_old
            // Example: aaronha01,1955,1,ML1,NL,153,153,602,105,189,37,9,27,106,3,1,49,61,5,3,7,4,20,153
            String entryPattern = "^(\\S+),(\\d{4}),(\\d+),(\\S+),(\\S+),(\\d+),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*),(\\d*)";

            Pattern p = Pattern.compile(entryPattern);
            Matcher matcher = p.matcher(value.toString());
            if (!matcher.matches()) {
                System.err.println("Bad record: " + value.toString());
                return;
            }

            // TODO STUDENT
        }
    }

    /**
     * Reducer class to add up all numbers associated with key.
     */
    public static class BattingDataProcessorReduce extends
            Reducer<Text, BattingWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<BattingWritable> values, Context context)
                throws IOException, InterruptedException {

            // TODO STUDENT
        }
    }

    /**
     * Constructs job and executes it.
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: <input_path> <output_path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf(), "batting-analysis");

        job.setJarByClass(BattingDataProcessor.class);

        job.setMapperClass(BattingDataProcessorMap.class);
        job.setReducerClass(BattingDataProcessorReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BattingWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Entry point for program.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BattingDataProcessor(), args);
        System.exit(res);
    }

}