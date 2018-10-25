package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.BooleanWritable;

import com.revature.map.MapperOne;
import com.revature.reduce.ReducerOne;

public class ProjectOne {

	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.out.printf("Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(ProjectOne.class);
		
		job.setJobName("Project One");
		
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapperOne.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		//Intermediate Reducer (Combiner)
		//job.setCombinerClass(SumReducer.class);
		
		//Output of combiner will be input of reducer
		job.setReducerClass(ReducerOne.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Boolean.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
