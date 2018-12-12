package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USFemaleEducationAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	private List<Double> changesInEduRate = new ArrayList<>();
	private List<Double> rates = new ArrayList<>();
	private double sumOfChanges = 0;
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		for(DoubleWritable value : values) {
				rates.add(value.get());
		}
		
		for(int i = 1; i < rates.size(); i++) {
			double diff = rates.get(i) - rates.get(i-1);
			changesInEduRate.add(diff);
		}

		for(int i = 0; i < changesInEduRate.size(); i++) {
			sumOfChanges += changesInEduRate.get(i);
		}
		context.write(key, new DoubleWritable(sumOfChanges/(double)changesInEduRate.size()));
	}
}
