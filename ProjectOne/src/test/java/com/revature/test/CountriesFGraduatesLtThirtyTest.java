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
import static com.revature.util.Utility.DoubleFormat;
import com.revature.CountriesFGraduatesLtThirty.AverageCountryFGraduatesLtThirtyReducer;
import com.revature.CountriesFGraduatesLtThirty.CountryFGraduatesMapper;

public class CountriesFGraduatesLtThirtyTest {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and
	 * a mapper and a reducer working together.
	 */
	private static MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private static ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private static MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	private static String input1 = "\"Bangladesh\",\"BGD\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER."
			+ "CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"\",\"\",\"\",\"3.16311\",\"3.37802\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6.32271\",\"\",\"\",\"\",\"\",";
	private static String input2 = "\"Jamaica\",\"JAM\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
	private static String input3 = "\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE."
			+ "ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\","
			+ "\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\","
			+ "\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"\",\"\",";

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@BeforeClass
	public static void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		CountryFGraduatesMapper mapper = new CountryFGraduatesMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		AverageCountryFGraduatesLtThirtyReducer reducer = new AverageCountryFGraduatesLtThirtyReducer();
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

		mapDriver.withOutput(new Text("Bangladesh"), new DoubleWritable(6.32271));
		mapDriver.withOutput(new Text("Bangladesh"), new DoubleWritable(3.37802));
		mapDriver.withOutput(new Text("Bangladesh"), new DoubleWritable(3.16311));

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
		values.add(new DoubleWritable(3.16311));
		values.add(new DoubleWritable(3.37802));
		values.add(new DoubleWritable(6.32271));

		reduceDriver.withInput(new Text("Bangladesh"), values);

		reduceDriver.withOutput(new Text("Bangladesh"), new DoubleWritable(DoubleFormat((3.16311 + 3.37802 + 6.32271)/3.0)));

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
		mapReduceDriver.addInput(new LongWritable(2), new Text(input2));
		mapReduceDriver.addInput(new LongWritable(3), new Text(input3));

		mapReduceDriver.addOutput(new Text("Bangladesh"), new DoubleWritable(DoubleFormat((3.16311 + 3.37802 + 6.32271)/3.0)));

		/*
		 * Run the test.
		 */
		mapReduceDriver.runTest();
	}
}