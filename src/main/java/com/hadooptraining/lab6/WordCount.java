package com.hadooptraining.lab6;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/************************************************************************
 *                                LAB 6                                 *
 ************************************************************************/

/**
 * The word count sample counts the number of word occurrences within a set of input documents
 * using MapReduce. The code has three parts: mapper, reducer, and the main program.
 */
public class WordCount {

    /**
     * The mapper extends from the org.apache.hadoop.mapreduce.Mapper interface. When Hadoop runs,
     * it receives each new line in the input files as an input to the mapper. The "map" function
     * tokenizes the line, and for each token (word) emits (word,1) as the output.
     * K1 = Object
     * V1 = Text
     * K2 = Text - the word itself
     * V2 = IntWritable - the number 1
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        // Create a static variable to store the number 1, since it is used many times
        private final static IntWritable one = new IntWritable(1);

        // Create a variable to store the word in the mapper
        private Text word = new Text();

        /**
         * The map function takes the document and splits up the words using blanks as the separator. For each
         * word found in the document, it emits a K2,V2 pair where K2 is the word, and V2 is the number 1.
         * @param key the key to emit
         * @param value the value to emit
         * @param context the context object to write to
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // TODO STUDENT
        }
    }

    /**
     * Reduce function receives all the values that has the same key as the input, and it outputs the key
     * and the number of occurrences of the key as the output.
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * The reduce function for word count, takes each word and counts the number of times that word occurs.
         * It emits the count as the value of that key.
         * @param key the word itself
         * @param values the number of times the word was found.
         * @param context the context object to write to
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {

            // TODO STUDENT
        }
    }

    /**
     * As input this program takes any text file. Entry point for the WordCount program.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // This is the configuration object.
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // If the number of arguments is not correct, print an error message and exit
        if (otherArgs.length != 2) {
            System.out.println("Usage: wordcount <input_hdfs_dir> <output_hdfs_dir>");
            System.out.println("Example: wordcount input output");
            System.exit(2);
        }

        // TODO STUDENT
    }
}
