package com.hadooptraining.extra.recommender.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

public class UserVectorToCooccurenceReducer
    extends Reducer<IntWritable, IntWritable, IntWritable, VectorWritable>
{
    private static final Logger logger = Logger.getLogger(UserVectorToCooccurenceReducer.class);

    public void reduce(IntWritable itemindex1, Iterable<IntWritable> itemIndex2s, Context context)
            throws IOException, InterruptedException {
        Vector cooccurenceRow = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        logger.debug("UserVectorToCooccurenceReducer itemIndex1 = " + itemindex1.toString());
        for (IntWritable intWritable : itemIndex2s) {
            int itemIndex2 = intWritable.get();
            logger.debug("UserVectorToCooccurenceReducer itemIndex2 = " + itemIndex2);
            cooccurenceRow.set(itemIndex2, cooccurenceRow.get(itemIndex2) + 1.0);
        }
        context.write(itemindex1, new VectorWritable(cooccurenceRow));
    }
}
