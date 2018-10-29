package com.revature.test;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.BeforeClass;
import org.junit.Test;
import static com.revature.util.Utility.*;
import com.revature.PercentChangeInMEmploymentFrom2000.*;


public class PercentChangeInMEmploymentTest {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and
	 * a mapper and a reducer working together.
	 */
	private static MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private static ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private static MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	private static String input1 = "\"World\",\"WLD\",\"Employment to population ratio, 15+, male (%) "
			+ "(modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
			+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"75.8417788946383\",\"\",\"75.428706359347\","
			+ "\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"73.7461080032518\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"71.9234108185349\","
			+ "\"\",\"\",\"72.0073062034121\",";

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@BeforeClass
	public static void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		MaleEmployment2000Mapper mapper = new MaleEmployment2000Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		PercentChangeMaleEmploymentReducer reducer = new PercentChangeMaleEmploymentReducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {

		mapDriver.withInput(new LongWritable(1), new Text(input1));

		mapDriver.addOutput(new Text(WORLD), new DoubleWritable(73.7461080032518));
		mapDriver.addOutput(new Text(WORLD), new DoubleWritable(71.9234108185349));
		mapDriver.addOutput(new Text(WORLD), new DoubleWritable(72.0073062034121));

		/*
		 * Run the test.
		 */
		mapDriver.runTest();
		
	}

	/*
	 * Test the reducer.
	 */
	@Test
	public void testReducer() {

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(73.7461080032518));
		values.add(new DoubleWritable(71.9234108185349));
		values.add(new DoubleWritable(72.0073062034121));

		reduceDriver.withInput(new Text(WORLD), values);

		reduceDriver.withOutput(new Text("Percent Change in Male Employment From 2000"), new DoubleWritable(DoubleFormat((72.0073062034121 - 73.7461080032518)/73.7461080032518)));

		/*
		 * Run the test.
		 */
		reduceDriver.runTest();
	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() {

		mapReduceDriver.addInput(new LongWritable(1), new Text(input1));


		mapReduceDriver.addOutput(new Text("Percent Change in Male Employment From 2000"), new DoubleWritable(DoubleFormat((72.0073062034121 - 73.7461080032518)/73.7461080032518)));

		/*
		 * Run the test.
		 */
		mapReduceDriver.runTest();
	}
}