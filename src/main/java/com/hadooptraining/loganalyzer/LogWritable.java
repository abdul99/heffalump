package com.hadooptraining.loganalyzer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custom class to write a line from a log file. A typical line looks like:
 *
 * tornado.umd.edu - - [28/Jul/1995:13:31:12 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1713
 *
 */
public class LogWritable implements WritableComparable<LogWritable> {
    private Text userIP, timestamp, request;
    private IntWritable status, responseSize;

    public LogWritable() {
        this.userIP = new Text();
        this.timestamp = new Text();
        this.request = new Text();
        this.status = new IntWritable();
        this.responseSize = new IntWritable();
    }

    public void set (String userIP, String timestamp, String request, int status, int bytes) {
        this.userIP.set(userIP);
        this.timestamp.set(timestamp);
        this.request.set(request);
        this.status.set(status);
        this.responseSize.set(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userIP.readFields(in);
        timestamp.readFields(in);
        request.readFields(in);
        status.readFields(in);
        responseSize.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        userIP.write(out);
        timestamp.write(out);
        request.write(out);
        status.write(out);
        responseSize.write(out);
    }

    @Override
    public int compareTo(LogWritable o) {
        if (userIP.compareTo(o.userIP) == 0) {
            return timestamp.compareTo(o.timestamp);
        } else
            return userIP.compareTo(o.userIP);
    }

    public int hashCode() {
        return userIP.hashCode();
    }

    public Text getUserIP() {
        return userIP;
    }

    public void setUserIP(Text userIP) {
        this.userIP = userIP;
    }

    public Text getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Text timestamp) {
        this.timestamp = timestamp;
    }

    public Text getRequest() {
        return request;
    }

    public void setRequest(Text request) {
        this.request = request;
    }

    public IntWritable getStatus() {
        return status;
    }

    public void setStatus(IntWritable status) {
        this.status = status;
    }

    public IntWritable getResponseSize() {
        return responseSize;
    }

    public void setResponseSize(IntWritable responseSize) {
        this.responseSize = responseSize;
    }
}