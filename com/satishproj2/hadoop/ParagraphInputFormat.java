package com.satishproj2.hadoop;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

@SuppressWarnings("rawtypes")
public class ParagraphInputFormat extends FileInputFormat<LongWritable, Text> {

	@SuppressWarnings("unchecked")
	@Override
	public RecordReader createRecordReader(InputSplit arg0,	TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new ParagraphRecordReader1();
	}

	

	

}
