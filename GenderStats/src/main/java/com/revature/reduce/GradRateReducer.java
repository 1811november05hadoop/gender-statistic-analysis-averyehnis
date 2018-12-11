package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GradRateReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		for(DoubleWritable value : values) {
			if(value.get() < 30.0) {
				context.write(key, value);
			}
		}
	}
}
