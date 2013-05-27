package com.hadooptraining.extra.recommender.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

public class UserVectorToCooccurenceMapper
    extends Mapper<VarLongWritable, VectorWritable, IntWritable, IntWritable> {
    private static final Logger logger = Logger.getLogger(UserVectorToCooccurenceMapper.class);

    public void map(VarLongWritable userID, VectorWritable userVector, Context context)
        throws IOException, InterruptedException {
        Iterator<Vector.Element> it = userVector.get().iterateNonZero();

        while (it.hasNext()) {
            int index1 = it.next().index();
            logger.debug("index1 = " + index1);
            Iterator<Vector.Element> it2 = userVector.get().iterateNonZero();

            while (it2.hasNext()) {
                int index2 = it2.next().index();
                logger.debug("index2 = " + index2);
                context.write(new IntWritable(index1), new IntWritable(index2));
            }
        }
    }
}
