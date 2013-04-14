package com.satishproj2.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

public class ParagraphRecordReader1 extends RecordReader<LongWritable, Text> {
	 private final int NLINESTOPROCESS = 3;
	    private LineReader in;
	    private LongWritable key;
	    private Text value = new Text();
	    private long start =0;
	    private long end =0;
	    private long pos =0;
	    private int maxLineLength;
	    
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (in != null) {
            in.close();
        }
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
        FileSystem fs = file.getFileSystem(conf);
        start = split.getStart();
        end= start + split.getLength();
        boolean skipFirstLine = false;
        FSDataInputStream filein = fs.open(split.getPath());
 
        if (start != 0){
            skipFirstLine = true;
            --start;
            filein.seek(start);
        }
        in = new LineReader(filein,conf);
        if(skipFirstLine){
            start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();
        final Text endline = new Text("\n\n");
        int newSize = 0;
        boolean flag=false;
            do {
            	Text v = new Text();
                newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
                
                pos=pos+newSize;
                int lengthOfv=v.getLength();
                if(lengthOfv==0)
                {
                	value.append(endline.getBytes(),0, endline.getLength());
                	v=new Text();
                	flag=true;
     	        	break;
                }
                value.append(v.getBytes(),0, v.getLength());
                //if (newSize < maxLineLength) {
               //     break;
               // }
            }while(newSize!=-1&& pos<end);

        if (flag == false) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
	}

}
