package com.hadooptraining.lab7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogWritable implements Writable {

    private Text userIP, timestamp, request;
    private IntWritable responseSize, status;

    public LogWritable() {
        this.userIP = new Text();
        this.timestamp =  new Text();
        this.request = new Text();
        this.responseSize = new IntWritable();
        this.status = new IntWritable();
    }

    public void set (String userIP, String timestamp, String request, int bytes, int status) {
        this.userIP.set(userIP);
        this.timestamp.set(timestamp);
        this.request.set(request);
        this.responseSize.set(bytes);
        this.status.set(status);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userIP.readFields(in);
        timestamp.readFields(in);
        request.readFields(in);
        responseSize.readFields(in);
        status.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        userIP.write(out);
        timestamp.write(out);
        request.write(out);
        responseSize.write(out);
        status.write(out);
    }

    public int hashCode()
    {
        return userIP.hashCode();
    }

    public Text getUserIP() {
        return userIP;
    }


    public Text getTimestamp() {
        return timestamp;
    }


    public Text getRequest() {
        return request;
    }


    public IntWritable getResponseSize() {
        return responseSize;
    }


    public IntWritable getStatus() {
        return status;
    }
}