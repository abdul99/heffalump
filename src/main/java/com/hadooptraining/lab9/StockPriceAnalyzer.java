package com.hadooptraining.lab9;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * After running the map function, if there are many key-value pairs with the same key, Hadoop has
 * to move all those values to the reduce function. This can incur a significant overhead. To
 * optimize such scenarios, Hadoop supports a special function called combiner . If provided,
 * Hadoop will call the combiner from the same node as the map node before invoking the
 * reducer and after running the mapper. This can significantly reduce the amount of data
 * transferred to the reduce step.
 *
 * This lab explains how to use the combiner in a stock price analysis program. Since the data
 * is large, using a combiner in such problems makes sense.
 *
 * This program uses the NYSE_daily data-set, which has a scheme as follows:
 * exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
 *
 * It finds the maximum high price for each stock over the course of all the data.
 */
public class StockPriceAnalyzer extends Configured implements Tool {

    /**
     * The mapper class receives <LongWritable, Text> as input <K1,V1> pair, and outputs
     * <Text, FloatWritable> as <K2,V2> pair. The output key is the stock symbol and the
     * output value is the high price of the stock.
     */
    public static class StockMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text stock = new Text();

        /**
         * Mapper to take an id as key and the stock price line as value. The incoming
         * key is of no use, so it is discarded. The output key-value is extracted from the
         * incoming value.
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Convert the value from Text to a String so we can use the StringTokenizer on it.
            String line = value.toString();

            // Split the line into fields, using comma as the delimiter
            StringTokenizer tokenizer = new StringTokenizer(line, ",");

            // We only care about the 2nd and 5th fields (stock_symbol and stock_price_high)
            String stock_symbol = null;
            String stock_price_high = null;

            // Simple loop to find out the values of interest to us
            for (int i = 0; i < 5 && tokenizer.hasMoreTokens(); i++) {
                switch (i) {
                    case 1: // The second field in data line
                        stock_symbol = tokenizer.nextToken();
                        break;
                    case 4: // The fifth field in the data line
                        stock_price_high = tokenizer.nextToken();
                        break;
                    default:
                        tokenizer.nextToken();
                        break;
                }
            }

            // Exit out of reducer when a bad record is found
            if (stock_symbol == null || stock_price_high == null) {
                // This is a bad record, throw it out and return
                System.err.println("Warning, bad record!");
                return;
            }

            // Discard the schema line at the head of each file
            if (stock_symbol.equals("stock_symbol")) {
                // Do nothing
            } else {
                // Set the key to be the stock symbol
                stock.set(stock_symbol);

                // Set the value to be the high price of the stock
                FloatWritable high = new FloatWritable(Float.valueOf(stock_price_high));

                // Write the key-value to the context object
                context.write(stock, high);
            }
        }
    }

    /**
     * The reducer receives <Text, FloatWritable> for every stock symbol. It then
     * finds the max of all received values and returns <K3,V3> pair as <Text, FloatWritable>.
     */
    public static class StockReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        /**
         * The reduce function, cycles through the values and finds the max value for
         * each key - the stock symbol.
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            // assume prices are never negative
            float max = 0.0f;

            // Loop through the values
            for (FloatWritable price : values) {
                float current = price.get();

                // Set the max value if current value found to be greater than current max
                if (current > max)
                    max = current;
            }

            // Write the key-value to the context object
            context.write(key, new FloatWritable(max));
        }
    }

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
        if (args.length < 2) {
            System.err.println("Usage: <input_path> <output_path>");
            System.exit(-1);
        }

        // Your job is handled by the Job object - managed by the JobTracker
        Job job = Job.getInstance(getConf(), "stock-analysis");

        // This locates the jar file that needs to be run by using a class name
        job.setJarByClass(StockPriceAnalyzer.class);

        // Set the mapper and reducer classes
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);

        // Set a combiner for the task
        job.setCombinerClass(StockReducer.class);

        // Set reducer output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // Set mapper output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

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
        int res = ToolRunner.run(new Configuration(), new StockPriceAnalyzer(), args);

        // Return the same exit code that was returned by ToolRunner.run()
        System.exit(res);
    }
}