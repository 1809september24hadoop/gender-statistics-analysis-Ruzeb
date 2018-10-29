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
import com.revature.PercentChangeInFEmployment.FemaleEmployment2000Mapper;
import com.revature.PercentChangeInFEmployment.PercentChangeFemaleEmploymentReducer;


public class PercentChangeInFEmploymentTest {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and
	 * a mapper and a reducer working together.
	 */
	private static MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private static ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private static MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	private static String input1 = "\"World\",\"WLD\",\"Employment to population ratio, 15+, female (%) "
			+ "(modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"49.0048849081483\",\"49.0952517511666\","
			+ "\"48.8900926643439\",\"48.8690980408558\",\"48.8425454291542\",\"48.6738302692562\","
			+ "\"48.5544856145047\",\"48.3131846507726\",\"48.4457130472085\",\"48.4789427159539\","
			+ "\"48.3973027356075\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\","
			+ "\"\",\"46.5597634947386\",\"46.5444447857213\",\"46.4441184118518\",";

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@BeforeClass
	public static void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		FemaleEmployment2000Mapper mapper = new FemaleEmployment2000Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		PercentChangeFemaleEmploymentReducer reducer = new PercentChangeFemaleEmploymentReducer();
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

		mapDriver.addOutput(new Text(WORLD), new DoubleWritable(48.4789427159539));
		mapDriver.addOutput(new Text(WORLD), new DoubleWritable(48.3973027356075));
		mapDriver.addOutput(new Text(WORLD), new DoubleWritable(46.5597634947386));
		mapDriver.addOutput(new Text(WORLD), new DoubleWritable(46.5444447857213));
		mapDriver.addOutput(new Text(WORLD), new DoubleWritable(46.4441184118518));

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
		values.add(new DoubleWritable(48.4789427159539));
		values.add(new DoubleWritable(48.3973027356075));
		values.add(new DoubleWritable(46.5597634947386));
		values.add(new DoubleWritable(46.5444447857213));
		values.add(new DoubleWritable(46.4441184118518));

		reduceDriver.withInput(new Text(WORLD), values);

		reduceDriver.withOutput(new Text("Percent Change in Female Employment From 2000"), new DoubleWritable(DoubleFormat((46.4441184118518 - 48.4789427159539)/48.4789427159539)));

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


		mapReduceDriver.addOutput(new Text("Percent Change in Female Employment From 2000"), new DoubleWritable(DoubleFormat((46.4441184118518 - 48.4789427159539)/48.4789427159539)));

		/*
		 * Run the test.
		 */
		mapReduceDriver.runTest();
	}
}