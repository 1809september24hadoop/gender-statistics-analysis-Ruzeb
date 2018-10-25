package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, DoubleWritable, Text, Boolean>{
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{

		Double totalPercentage = 0.0;
		
		Double counter = 0.0;
		for (DoubleWritable value: values){
			totalPercentage += value.get();
			counter+= 1.0;
		}
		
		totalPercentage /= counter;
		if (totalPercentage < 30.0){
			context.write(key, new Boolean(true));
		} 
	}

}
