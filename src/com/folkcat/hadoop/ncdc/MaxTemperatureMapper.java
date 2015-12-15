package com.folkcat.hadoop.ncdc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	private NcdcRecordParser parser=new NcdcRecordParser();
	
	@Override
	public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
		//context.write(new Text("开始啦"),new IntWritable(0));
		
		/*
		String line=value.toString();
		String year=line.substring(15, 19);
		int airTemp=Integer.parseInt(line.substring(87,92));
		context.write(new Text(year), new IntWritable(airTemp));
		*/
		
		//Parser 版本
		parser.parse(value);
		if(parser.isValidTemperature()){
			context.write(new Text(parser.getYear()),new IntWritable(parser.getAirTemperature()));
		}
	
		
	}
	
	
}


