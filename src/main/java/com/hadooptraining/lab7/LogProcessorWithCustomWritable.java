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

/************************************************************************
 *                                LAB 7                                 *
 ************************************************************************/

/**
 * When none of the built-in data types matches your requirements, one needs
 * to write a custom writable data type. This is done by implementing the
 * org.apache.hadoop.io.Writable interface to define the serialization
 * format of your data type. The Writable interface-based types can be
 * used as value types in Hadoop MapReduce computations.
 */
public class LogProcessorWithCustomWritable extends Configured implements Tool {

    /**
     * Mapper class that takes <K1,V1> pair as <LongWritable, Text> and writes
     * a <K2,V2> pair of <Text, LogWritable>.
     */
    public static class LogProcessorMap extends
            Mapper<LongWritable, Text, Text, LogWritable> {

        // The following two are the <K,V> pairs for the mapper.
        // We reuse these variables for each call to map() function.
        private Text userHostText = new Text();
        private LogWritable logValue = new LogWritable();

        /**
         * The mapper that takes <K1,V1> as <LongWritable, Text>. This function
         * parses the log line, extracts the values and sets the appropriate values
         * in the custom writable object by using some of the parsed values.
         * @param key the incoming key
         * @param value the incoming value
         * @param context the context object
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // prepare the log pattern string
            String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";
            // Example: unicomp6.unicomp.net - - [01/Jul/1995:00:00:14 -0400] "GET /shuttle/countdown/count.gif HTTP/1.0" 200 40310

            // TODO STUDENT

        }
    }

    /**
     * The Reducer for the job. It takes <K2,V2> as <Text, LogWritable> and emits <K3,V3> as <Text,IntWritable>.
     */
    public static class LogProcessorReduce extends
            Reducer<Text, LogWritable, Text, IntWritable> {
        // Create a common IntWritable object to hold the result
        private IntWritable result = new IntWritable();

        /**
         * The reducer looks at the incoming <K,V> pair, iterates through the value and extracts
         * the response size by invoking the LogWritable.getResponseSize() method. Then it
         * sums up those numbers and writes the sum to the context object.
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<LogWritable> values,  Context context)
                throws IOException, InterruptedException {

            // TODO STUDENT
        }
    }

    /**
     * The run method for constructing your job. This is required according to the Tools interface.
     * The Tools interface helps constructing a Hadoop job that needs  reading, parsing, and
     * processing command-line arguments.
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        // If the number of arguments is insufficient, print an error message and exit
        if (args.length < 2) {
            System.err.println("Usage: <input_path> <output_path>");
            System.exit(-1);
        }

        // Your job is handled by the Job object - managed by the JobTracker
        Job job = Job.getInstance(getConf(), "log-analysis");

        // This locates the jar file that needs to be run by using a class name
        job.setJarByClass(LogProcessorWithCustomWritable.class);

        // TODO STUDENT

        // Add the input and output paths from program arguments
        FileInputFormat.setInputPaths(job, new Path(args[0]));
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
        int res = ToolRunner.run(new Configuration(), new LogProcessorWithCustomWritable(), args);

        // Return the same exit code that was returned by ToolRunner.run()
        System.exit(res);
    }

}