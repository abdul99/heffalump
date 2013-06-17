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

/************************************************************************
 *                                LAB 15                                *
 ************************************************************************/
/**
 * This class adds up all the runs allowed by the player in their lifetime. We use the
 * playerID as the key for the MapReduce job. The value is a custom writable object called
 * PitchingWritable.
 */
public class PitchingDataProcessor extends Configured implements Tool {

    public static class PitchingDataProcessorMap extends
            Mapper<LongWritable, Text, Text, PitchingWritable> {

        private Text playerIDText = new Text();
        private PitchingWritable pitchingValue = new PitchingWritable();

        /**
         * Mapper class using 'playerID' as key and 'PitchingWritable' as VALUE.
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Format: playerID,yearID,stint,teamID,lgID,W,L,G,GS,CG,SHO,SV,IPouts,H,ER,HR,BB,SO,BAOpp,ERA,IBB,WP,HBP,BK,BFP,GF,R,SH,SF,GIDP
            // Example: aardsda01,2010,1,SEA,AL,0,6,53,0,0,0,31,149,33,19,5,25,49,,3.44,5,2,2,0,202,43,19,,,

            // TODO STUDENT
        }
    }

    /**
     * Reducer class to add up all numbers associated with key.
     */
    public static class PitchingDataProcessorReduce extends
            Reducer<Text, PitchingWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<PitchingWritable> values, Context context)
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

        Job job = Job.getInstance(getConf(), "pitching-analysis");

        job.setJarByClass(PitchingDataProcessor.class);

        job.setMapperClass(PitchingDataProcessorMap.class);
        job.setReducerClass(PitchingDataProcessorReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PitchingWritable.class);

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
        int res = ToolRunner.run(new Configuration(), new PitchingDataProcessor(), args);
        System.exit(res);
    }

}
