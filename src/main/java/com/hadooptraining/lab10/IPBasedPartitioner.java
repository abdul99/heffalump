package com.hadooptraining.lab10;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The Partitioner is based on the IP address. We merely tokenize the value
 * and use the hashCode of the first component to get the partition id.
 */
public class IPBasedPartitioner extends Partitioner<Text, IntWritable> {

    /**
     * Implementation of the getPartition() function, required by the abstract
     * class Partioner<KEY,VALUE>.
     * @param ipAddress
     * @param value
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text ipAddress, IntWritable value, int numPartitions) {
        // Tokenize the IP address by breaking it into its 4 components
        StringTokenizer tokenizer  = new StringTokenizer(ipAddress.toString(), ".");
        if (tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
            // return a partition Id based on first component of IP address
            return ((token.hashCode() & Integer.MAX_VALUE) % numPartitions);
        }
        return 0;
    }
}
