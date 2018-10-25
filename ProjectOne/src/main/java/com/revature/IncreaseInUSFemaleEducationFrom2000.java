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

public class IncreaseInUSFemaleEducationFrom2000 {

	public static class USFemaleEducationFrom2000Mapper extends Mapper<Object, Text, Text, DoubleWritable>{

		public static final String PRIMARYEDUCATIONFEMALES = "SE.PRM.NENR.FE";


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String record = value.toString();
			String recordTrim = record.substring(1, record.length()-2);
			String[] columnSplit = recordTrim.split("\",\"");
			
			if (columnSplit[1].equals("USA")){
				if (columnSplit[3].equals(PRIMARYEDUCATIONFEMALES)){
					System.out.println("HERE");
					for (int index = 44; index < columnSplit.length; index++){
						String data = columnSplit[index];
						if (!data.equals("")){
							context.write(new Text(columnSplit[0]), new DoubleWritable(Double.parseDouble(data)));
						}
					}
				}
			}

//			if (columnSplit[2].equals("USA")){
//				if (columnSplit[3].equals(PRIMARYEDUCATIONFEMALES)){
//					for (int index = 44, next = 45; index < columnSplit.length-1 || next < columnSplit.length-1; index++, next++){
//						//Integer year = new Integer(2000);
////						if (!columnSplit[index].trim().equals("")){
////							context.write(new Text(columnSplit[0]), 
////									new TupleWritable(new Writable[]{new IntWritable(year++), 
////											new DoubleWritable(Double.parseDouble(columnSplit[index]))}));
////						}
//						if (!columnSplit[index].trim().equals("")){
//							Double precedent = Double.parseDouble(columnSplit[index]);
//							index++;
//							while(!columnSplit[next].trim().equals("") && next < columnSplit.length-1){
//								next++;
//							}
//							Double current = Double.parseDouble(columnSplit[index]);
//							context.write(new Text(columnSplit[0]), new DoubleWritable(current - precedent));
//						}
//					}
//				}
//			}	
		}	
	}

	public static class IncreaseInEducationReducer extends Reducer<Text, DoubleWritable, Text, Double>{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			
			double differences = 0.0;
			double counter = 0.0;
			double average = 0.0;
			double previous = 0.0;
			
			if (values.iterator().hasNext()){
				previous = values.iterator().next().get();	
			}
			while (values.iterator().hasNext()){
				double last = previous;
				double next = values.iterator().next().get();
				previous = next;
				differences += last - next;
				counter++;
			}
			
			average = differences / counter;
			context.write(new Text("USA"), average);
			
//			for (DoubleWritable dr : values){
//				context.write(new Text("USA"), dr.get());
//			}
		}
	}

	public static void main(String[] args) throws Exception{
		Job job = new Job();
		job.setJarByClass(IncreaseInUSFemaleEducationFrom2000.class);
		job.setJobName("Countries With female graduates < 30%");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setMapperClass(USFemaleEducationFrom2000Mapper.class);
		job.setReducerClass(IncreaseInEducationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Double.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}


}
