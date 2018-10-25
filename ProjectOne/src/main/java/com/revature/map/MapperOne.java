package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//LongWritable, Text, Text, IntWritable
public class MapperOne extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	public static final String BACHELORS25 = "SE.TER.HIAT.BA.FE.ZS";
	public static final String MASTERS25 = "SE.TER.HIAT.MS.FE.ZS";
	public static final String DOCTORATES25 = "SE.TER.HIAT.DO.FE.ZS";
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] columnSplit = value.toString().substring(1, value.toString().length()-1).split("\",\"");
		
		if (columnSplit[3].equals(BACHELORS25) || columnSplit[3].equals(MASTERS25) || columnSplit[3].equals(DOCTORATES25)){
			for (int index = 4; index < 60; index++){
//				if(!(columnSplit[index].length() > 0)){
//					context.write(new Text(columnSplit[0]), new DoubleWritable(0.0));
//				} else {
//					context.write(new Text(columnSplit[0]), new DoubleWritable(Double.parseDouble(columnSplit[index])));
//				}
				
				
				
				
				
				if (!columnSplit[index].trim().equals("")){
					context.write(new Text(columnSplit[0]), new DoubleWritable(Double.parseDouble(columnSplit[index])));
				}
			}
		}
		
	}
}
	


