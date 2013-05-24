package com.hadooptraining.recommender.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VarLongWritable;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikipediaToItemPrefsMapper
        extends Mapper<LongWritable, Text, VarLongWritable, VarLongWritable> {
    private static final Pattern NUMBERS = Pattern.compile("(\\d+)");
    private static final Logger logger = Logger.getLogger(WikipediaToItemPrefsMapper.class);

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        logger.debug("Incoming line is: " + line);
        Matcher m = NUMBERS.matcher(line);
        m.find();

        VarLongWritable userID = new VarLongWritable(Long.parseLong(m.group()));
        logger.debug("userID is: " + userID.toString());
        VarLongWritable itemID = new VarLongWritable();
        logger.debug("itemID is: " + itemID.toString());

        while (m.find()) {
            itemID.set(Long.parseLong(m.group()));
            context.write(userID, itemID);
        }
    }
}
