package com.hadooptraining.lab8;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * LogFileInputFormat extends the FileInputFormat, which provides a generic splitting
 * mechanism for HDFS-file based InputFormat. We override the createRecordReader()
 * method in the LogFileInputFormat to provide an instance of our custom
 * RecordReader implementation, LogFileRecordReader. Optionally, we can also
 * override the isSplitable() method of the FileInputFormat to control
 * whether the input files are split-up into logical partitions or used as whole files.
 */
public class LogFileInputFormat extends FileInputFormat<LongWritable, LogWritable>{

    /**
     * Returns an instance of a custom record reader.
     * @param arg0
     * @param arg1
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<LongWritable, LogWritable> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new LogFileRecordReader();
    }
}
