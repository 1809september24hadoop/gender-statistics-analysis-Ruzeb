package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PercentChangeInMaleEmploymentFrom2000 {

	public static class MaleEducation2000Mapper extends Mapper<Object, Text, Text, DoubleWritable>{

		public static final String WorldMaleEducationFrom2000 = "SL.EMP.TOTL.SP.MA.ZS";


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String record = value.toString();
			String recordTrim = record.substring(1, record.length()-2);
			String[] columnSplit = recordTrim.split("\",\"");
			
			if (columnSplit[1].equals("WLD")){
				if (columnSplit[3].equals(WorldMaleEducationFrom2000)){
//					for (int index = 44; index < columnSplit.length; index++){
//						if (!columnSplit[index].equals("")){
//							context.write(new Text(columnSplit[0]), new DoubleWritable(Double.parseDouble(data)));
//						}
//					}
					context.write(new Text(columnSplit[0]), new DoubleWritable(Double.parseDouble(columnSplit[columnSplit.length-1])));
					context.write(new Text(columnSplit[0]), new DoubleWritable(Double.parseDouble(columnSplit[44])));
				}
			}
		}	
	}

	public static class PercentChangeMaleEmploymentReducer extends Reducer<Text, DoubleWritable, Text, Double>{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			
			double Y2K = 0.0;
			double Y2K16 = 0.0;
			
			if (values.iterator().hasNext()){
				Y2K = values.iterator().next().get();
			}
			if (values.iterator().hasNext()){
				Y2K16 = values.iterator().next().get();
				if (Y2K16 == 0.0){
					//throw new Exception
				}
			}
	
			
			context.write(new Text("USA"), (Y2K16-Y2K)/Y2K);
			
//			for (DoubleWritable dr : values){
//				context.write(new Text("USA"), dr.get());
//			}
		}
	}

	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(PercentChangeInMaleEmploymentFrom2000.class);
		job.setJobName("Countries With female graduates < 30%");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MaleEducation2000Mapper.class);
		job.setReducerClass(PercentChangeMaleEmploymentReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Double.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}


}

