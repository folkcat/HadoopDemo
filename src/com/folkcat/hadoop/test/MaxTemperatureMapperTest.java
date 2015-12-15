package com.folkcat.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.folkcat.hadoop.ncdc.MaxTemperatureMapper;

public class MaxTemperatureMapperTest {
	@Test
	public void processesValidRecord() throws IOException,
			InterruptedException {
		LongWritable k=new LongWritable(0);
		Text value = new Text(
				"0149010010999991990010218004+70933-008667FM-12+0009ENJA V0201701N00311000301CN0005001N9-00021-00031100921ADDAA199003091AG10000AY161061AY251061GA1091+999999999GF109991091999999999999999KA1120M+00221MD1610091+9999MW1601OA149901131REMSYN017333   10022 91122");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
				.withMapper(new MaxTemperatureMapper())
				.withInput(k,value)
				.withOutput(new Text("1990"),new IntWritable(-2))
				.runTest();
	}
}
