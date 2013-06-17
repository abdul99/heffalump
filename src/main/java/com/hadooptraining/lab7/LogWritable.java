package com.hadooptraining.lab7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custom writable class that holds data from a log file. The calling object must pass
 * five values to this object - values that are typically extracted by parsing a log line.
 */
public class LogWritable implements Writable {

    // Define the fields stored within this object
    private Text userIP, timestamp, request;
    private IntWritable responseSize, status;

    /**
     * Default constructor. Creates empty fields.
     */
    public LogWritable() {
        this.userIP = new Text();
        this.timestamp =  new Text();
        this.request = new Text();
        this.responseSize = new IntWritable();
        this.status = new IntWritable();
    }

    /**
     * Set the values of all fields in this object.
     * @param userIP
     * @param timestamp
     * @param request
     * @param bytes
     * @param status
     */
    public void set (String userIP, String timestamp, String request, int bytes, int status) {
        // TODO STUDENT
    }

    /**
     * Given a DataInput object, this method will read its fields from DataInput.
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO STUDENT
    }

    /**
     * Given a DataOutput object, this method will write its values to DataOutput.
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        // TODO STUDENT
    }

    /**
     * A method used to calculate assignment of reducers based on key.
     * @return
     */
    public int hashCode()
    {
        // TODO STUDENT
        return userIP.hashCode();
    }

    /**
     * Get user IP as a string.
     * @return
     */
    public Text getUserIP() {
        return userIP;
    }


    /**
     * Get time stamp as text.
     * @return
     */
    public Text getTimestamp() {
        return timestamp;
    }


    /**
     * Get request string.
     * @return
     */
    public Text getRequest() {
        return request;
    }

    /**
     * Get response size in bytes.
     * @return
     */
    public IntWritable getResponseSize() {
        return responseSize;
    }

    /**
     * Get status code of response as an integer.
     * @return
     */
    public IntWritable getStatus() {
        return status;
    }
}