package com.folkcat.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.folkcat.hadoop.wc.WordCountMapper;

public class WordCountTest {
	@Test
	public void processesValidRecord() throws IOException,
			InterruptedException {
		LongWritable k=new LongWritable(85456);
		Text value = new Text("hello hello");
		new MapDriver<Object, Text, Text, IntWritable>()
				.withMapper(new WordCountMapper())
				.withInput(k,value)
				.withOutput(new Text("hello"),new IntWritable(1))
				.withOutput(new Text("hello"),new IntWritable(1))
				.runTest();
	}
}
