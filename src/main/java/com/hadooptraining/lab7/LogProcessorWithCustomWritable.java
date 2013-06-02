package com.hadooptraining.lab7;

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

public class LogProcessorWithCustomWritable extends Configured implements Tool {

    public static class LogProcessorMap extends
            Mapper<LongWritable, Text, Text, LogWritable> {

        private Text userHostText = new Text();
        private LogWritable logValue = new LogWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";
            // Example: unicomp6.unicomp.net - - [01/Jul/1995:00:00:14 -0400] "GET /shuttle/countdown/count.gif HTTP/1.0" 200 40310

            Pattern p = Pattern.compile(logEntryPattern);
            Matcher matcher = p.matcher(value.toString());
            if (!matcher.matches()) {
                return;
            }

            logValue.set(matcher.group(1), matcher.group(4), matcher.group(5), Integer.parseInt(matcher.group(7)),
                    Integer.parseInt(matcher.group(6)));
            userHostText.set(matcher.group(1));

            context.write(userHostText, logValue);
        }
    }

    public static class LogProcessorReduce extends
            Reducer<Text, LogWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<LogWritable> values,  Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (LogWritable logLine : values) {
                sum += logLine.getResponseSize().get();
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

        Job job = Job.getInstance(getConf(), "log-analysis");

        job.setJarByClass(LogProcessorWithCustomWritable.class);

        job.setMapperClass(LogProcessorMap.class);
        job.setReducerClass(LogProcessorReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LogWritable.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LogProcessorWithCustomWritable(), args);
        System.exit(res);
    }

}