package com.hadooptraining.extra.recommender.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

public class WikipediaToUserVectorReducer
        extends Reducer<VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable> {
    private static final Logger logger = Logger.getLogger(WikipediaToUserVectorReducer.class);

    public void reduce(VarLongWritable userID, Iterable<VarLongWritable> itemPrefs, Context context)
        throws IOException, InterruptedException {
        Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        for (VarLongWritable itemPref : itemPrefs) {
            userVector.set((int)itemPref.get(), 1.0f);
        }
        context.write(userID, new VectorWritable(userVector));
    }
}
