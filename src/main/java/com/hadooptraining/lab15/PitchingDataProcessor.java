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

public class PitchingDataProcessor extends Configured implements Tool {

    public static class PitchingDataProcessorMap extends
            Mapper<LongWritable, Text, Text, PitchingWritable> {

        private Text playerIDText = new Text();
        private PitchingWritable pitchingValue = new PitchingWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Format: playerID,yearID,stint,teamID,lgID,W,L,G,GS,CG,SHO,SV,IPouts,H,ER,HR,BB,SO,BAOpp,ERA,IBB,WP,HBP,BK,BFP,GF,R,SH,SF,GIDP
            // Example: aardsda01,2010,1,SEA,AL,0,6,53,0,0,0,31,149,33,19,5,25,49,,3.44,5,2,2,0,202,43,19,,,

            String[] fields = value.toString().split(",");

            if ("playerID".equalsIgnoreCase(fields[0])) {
                // This simple check eliminates the very first record
                return;
            }

            if ((fields[26] == null) || (fields[26].isEmpty()))
                return;

            pitchingValue.set(fields[0], fields[1], Integer.parseInt(fields[26]));
            playerIDText.set(fields[0]);

            context.write(playerIDText, pitchingValue);
        }
    }

    public static class PitchingDataProcessorReduce extends
            Reducer<Text, PitchingWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<PitchingWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (PitchingWritable line : values) {
                sum += line.getRunsAllowed().get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: <input_path> <output_path>");
            System.exit(-1);
        }

		/* input parameters */
        String inputPath = args[0];
        String outputPath = args[1];

        Job job = Job.getInstance(getConf(), "pitching-analysis");

        job.setJarByClass(PitchingDataProcessor.class);

        job.setMapperClass(PitchingDataProcessorMap.class);
        job.setReducerClass(PitchingDataProcessorReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PitchingWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PitchingDataProcessor(), args);
        System.exit(res);
    }

}
