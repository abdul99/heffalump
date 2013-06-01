package com.hadooptraining.lab5;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/************************************************************************
 *                                LAB 5                                 *
 ************************************************************************/

/**
 * Most of the text searching systems rely on inverted index to look up the set of
 * documents that contains a given word or a term. In this example, we are going
 * to build a simple inverted index that computes a list of terms in the documents,
 * the set of documents that contains each term, and the term frequency in each of
 * the documents. Retrieval of results from an inverted index can be as simple
 * as returning the set of documents that contains the given terms or can involve
 * much more complex operations such as returning the set of documents ordered
 * based on a particular ranking.
 *
 * This class differs from the InvertedIndex class only in one aspect. We do not
 * specify any OutputFormat in this class, thereby allowing the default TextOutputFormat
 * class to be used for the output.
 */

public class TextOutInvertedIndexer {

   /**
     * Map Function receives a chunk of an input document as the input and
     * outputs the term and the pair [docid, 1] for each word. We can use a combiner
     * to optimize the intermediate data communication. Note that mappers must
     * extend the class Mapper with four parameters <K1, V1, K2, V2> where
     * K1 = input key type for Mapper
     * V1 = input value type for Mapper
     * K2 = output key type for Mapper
     * V2 = output value type for Mapper
     *
     * Reducers on the other hand take <K2, V2> as input and yield another set of keys <K3, V3>
     * as output.
     */
    public static class IndexingMapper extends
            Mapper<Object, Text, Text, TermFrequencyWritable> {

        private TermFrequencyWritable docFrequency = new TermFrequencyWritable();
        private Text term = new Text();

       /**
        * This is the mapper for the inverted index. Note that the key and value are
        * the same ones expressed in K2 and V2 in the template above.
        * @param key output key for the mapper
        * @param value output value for the mapper
        * @param context the context object that stores the configuration and handles the writes
        * @throws IOException
        * @throws InterruptedException
        */
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String valString = value.toString().replaceAll("[^a-zA-Z0-9]+"," ");
            StringTokenizer itr = new StringTokenizer(valString);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            while (itr.hasMoreTokens()) {
                term.set(itr.nextToken());
                docFrequency.set(fileName, 1);
                context.write(term, docFrequency);
            }
        }
    }

    /**
     * Reduce function receives IDs and frequencies of all the documents that
     * contains the term (Key) as the input. Reduce function outputs the term
     * and a list of document IDs and the number of occurrences of the term in
     * each document as the output.
     */
    public static class IndexingReducer extends
            Reducer<Text, TermFrequencyWritable, Text, Text> {

        /**
         * The reduce function takes the document id as key, and frequency of the document as value.
         * It outputs the term and a list of document ids and the number of occurance of the term
         * in that document. The signature of the the reduce function is <K3, V3>
         * @param key the output key for the reducer
         * @param values the output value for the reducer
         * @param context a context object to write to
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<TermFrequencyWritable> values,
                           Context context) throws IOException, InterruptedException {

            HashMap<Text, IntWritable> map = new HashMap<Text, IntWritable>();
            for (TermFrequencyWritable val : values) {
                Text docID = new Text(val.getDocumentID());
                int freq = val.getFreq().get();
                if (map.get(docID) != null) {
                    map.put(docID, new IntWritable(map.get(docID).get() + freq));
                } else {
                    map.put(docID, new IntWritable(freq));
                }
            }

            Iterator<Entry<Text, IntWritable>> it = map.entrySet().iterator();
            StringBuilder strBuilder = new StringBuilder();
            while (it.hasNext()) {
                Entry<Text, IntWritable> pair = it.next();
                strBuilder.append(pair.getKey() + ":" + pair.getValue() + ",");
                it.remove(); // avoids a ConcurrentModificationException
            }
            context.write(key, new Text(strBuilder.toString()));
        }
    }

    /**
     * This is the combiner class for the MapReduce operation. The combiner is called before
     * it is passed on to the partitioner. Combiners make the MapReduce operation go faster
     * since it acts as the map-side reduce operation.
     */
    public static class IndexingCombiner extends
            Reducer<Text, TermFrequencyWritable, Text, TermFrequencyWritable> {

        /**
         * Note that the signature of the combiner is the same as the reduce function. It uses
         * <K3, V3> as its key-value pairs.
         * @param key the combiner output key
         * @param values the combiner output value
         * @param context the context object to write to
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<TermFrequencyWritable> values,
                           Context context) throws IOException, InterruptedException {

            int count = 0;
            String id = "";
            for (TermFrequencyWritable val : values) {
                count++;
                if (count == 1) {
                    id = val.getDocumentID().toString();
                }
            }

            TermFrequencyWritable writable = new TermFrequencyWritable();
            writable.set(id, count);
            context.write(key, writable);
        }
    }

    /**
     * This is the main entry point for a Map-Reduce program. The steps below constitute a familiar boiler-plate code.
     * You just need to configure your map-reduce program by assigning the appropriate classes to do the job.
     * Consider the framework as an engine with plenty of hooks around it. To run your job you need to write your
     * class and attach your class to those hooks. Your class gets called by the framework at the appropriate time
     * and your function gets executed. What machine your code executes in, and in what order, is not in your control,
     * rather it is handled by the framework depending on the configuration. Your results would appear in the
     * specified output folder.
     *
     * @param args the arguments for the main function
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // This is the configuration object.
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TextOutInvertedIndexer <in> <out>");
            System.exit(2);
        }

        // Your job is handled by the Job object - managed by the JobTracker
        Job job = new Job(conf, "TextOut Inverted Indexer");

        // This is class that is used to find the jar file that needs to be run
        job.setJarByClass(TextOutInvertedIndexer.class);

        // Set the Map and Reduce classes
        job.setMapperClass(IndexingMapper.class);
        job.setReducerClass(IndexingReducer.class);

        // Set the combiner class
        job.setCombinerClass(IndexingCombiner.class);

        // The the Reducer output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the map classes
        job.setMapOutputValueClass(TermFrequencyWritable.class);

        // NOTE: We do not need to set an OutputFormat if we want text output. Default is TextOutputFormat

        // Set the input and output folders
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // The exit code is determined upon the success of the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
