package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GradRateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] splitLine = line.split("\",\"");

		if(line.contains("at least completed upper secondary") && line.contains("female")) {
			for(int i = splitLine.length-1; i >= 4; i--) {
				if (splitLine[i].matches("[0-9]")) {
					context.write(new Text(splitLine[0]), new DoubleWritable(Double.valueOf(splitLine[i])));
				}
			}
		}
	}
}
