package com.satishproj2.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Pair, IntWritable>{

	@Override
	public int getPartition(Pair arg0, IntWritable arg1, int numOfReduceTasks) {
		Text firstWord= new Text();
		firstWord=arg0.firstWord;
		return firstWord.hashCode()%numOfReduceTasks;
	}

}
