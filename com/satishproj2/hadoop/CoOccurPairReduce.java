package com.satishproj2.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CoOccurPairReduce extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	boolean flag=false;
	double totalCount=0;
		


	@Override
	protected void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		
		if(key.secondWord.compareTo(new Text("*"))==0)
		{	
			for (IntWritable i : values) {
				totalCount += i.get();
			}				
			context.write(key, new DoubleWritable(totalCount));
		}
		else
		{
			
			for (IntWritable i : values) 
			{
				sum += i.get();
			}
			double freq=((double)sum)/(totalCount);
			
			context.write(key, new DoubleWritable(freq));
		}
	}
	}
		
