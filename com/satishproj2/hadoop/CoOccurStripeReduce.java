package com.satishproj2.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class CoOccurStripeReduce extends Reducer<Text, MapWritable, Text, DoubleWritable> {
	private MapWritable stripe = new MapWritable();

	@Override
	protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		stripe.clear();
		IntWritable totalCount,oneCount;
		double total=0;
		Iterator<MapWritable> ii=values.iterator();
		
		MapWritable map=new MapWritable();
	
		
		for (MapWritable MWvalue : values) 
		{
			
			Set<Writable> keySet = MWvalue.keySet();
			for (Writable keyInStripe : keySet) {
				
				
				totalCount = (IntWritable) MWvalue.get(keyInStripe);
				total=total+totalCount.get();
				map.put(keyInStripe, totalCount);

			}
		}
		Set<Writable> myKey=map.keySet();
		for(Writable someKey:myKey)
		{
			oneCount=(IntWritable)map.get(someKey);
			
			context.write( new Text (key+","+someKey) ,new DoubleWritable((double)oneCount.get()/total) );
		}
	}

}



