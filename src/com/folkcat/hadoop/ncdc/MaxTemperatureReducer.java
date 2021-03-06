package com.folkcat.hadoop.ncdc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	@Override
	public void reduce(Text key,Iterable<IntWritable> values,Context ctx)throws IOException,InterruptedException{
		//ctx.write(new Text("SHABI"), new IntWritable(200));
		int maxValue=Integer.MIN_VALUE;
		for(IntWritable value:values){
			maxValue=Math.max(maxValue, value.get());
			//ctx.write(key, value);
		}
		ctx.write(key,new IntWritable(maxValue));
	}

}
