package com.hadooptraining.extra.indexer;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CompanyProfileIndexer extends Configured implements Tool {

    static class ProfileMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text url = new Text();
        private Text product = new Text();

        public static enum PROFILE_COUNTER {
            BAD_RECORDS,
            PROCCESSED_RECORDS
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            url.set(itr.nextToken());
            while (itr.hasMoreTokens()) {
                product.set(itr.nextToken());
                output.collect(product, url);
            }
        }
    }

    static class ProfileReducer extends Reducer<Text, Text, Text, Text> {

        private Text urls = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            StringBuilder sb = new StringBuilder();
            while (values.hasNext()) {
                sb.append(" ");
                sb.append(values.next().toString());
            }
            urls.set(sb.toString());
            output.collect(key, urls);
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: CompanyProfileIndexer <in-dir> <out-dir>");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Job job = new Job(getConf(), "company-profile-index");

        job.setJarByClass(CompanyProfileIndexer.class);
        job.setMapperClass(ProfileMapper.class);
        job.setReducerClass(ProfileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // job.setInputFormatClass(LogFileInputFormat.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);

        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        int exitStatus = job.waitForCompletion(true) ? 0 : 1;

        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job took " + ((endTime.getTime() - startTime.getTime()) / 1000) + " seconds.");
        return exitStatus;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CompanyProfileIndexer(), args);
        System.exit(res);
    }

}