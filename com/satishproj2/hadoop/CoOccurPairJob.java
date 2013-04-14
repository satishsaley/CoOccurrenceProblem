package com.satishproj2.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class CoOccurPairJob {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "cooccurrence-java");
		job.setJarByClass(CoOccurPairJob.class);

		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		//job.setPartitionerClass(MyPartitioner.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(CoOccurPairMap.class);
		//job.setCombinerClass(CoOccurReduce.class);
		job.setReducerClass(CoOccurPairReduce.class);
		//job.setNumReduceTasks(1);
		 
		job.setInputFormatClass(ParagraphInputFormat.class);
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new
				Path(args[1]));

		job.waitForCompletion(true);

		}
}
