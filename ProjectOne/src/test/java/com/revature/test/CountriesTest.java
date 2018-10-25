package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.revature.CountriesFGraduatesLtThirty.AverageCountryFGraduatesLtThirtyReducer;
import com.revature.CountriesFGraduatesLtThirty.CountryFGraduatesMapper;
import com.revature.IncreaseInUSFemaleEducationFrom2000.USFemaleEducationFrom2000Mapper;

public class CountriesTest {

  /*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  private static MapDriver<Object, Text, Text, DoubleWritable> mapDriver;
  private static ReduceDriver<Text, DoubleWritable, Text, Boolean> reduceDriver;
  private static MapReduceDriver<Object, Text, Text, DoubleWritable, Text, Boolean> mapReduceDriver;

  /*
   * Set up the test. This method will be called before every test.
   */
  @BeforeClass
  public static void setUp() {

    /*
     * Set up the mapper test harness.
     */
	USFemaleEducationFrom2000Mapper mapper = new USFemaleEducationFrom2000Mapper();
    mapDriver = new MapDriver<Object, Text, Text, DoubleWritable>();
    mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
    AverageCountryFGraduatesLtThirtyReducer reducer = new AverageCountryFGraduatesLtThirtyReducer();
    reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, Boolean>();
    reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
    mapReduceDriver = new MapReduceDriver<Object, Text, Text, DoubleWritable, Text, Boolean>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {

    /*
     * For this test, the mapper's input will be "1 cat cat dog" 
     */
    //mapDriver.withInput(new Object(), new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\","));
	  //mapDriver.withInput(new Object(), new Text("\"United States\",\"USA\",\"School enrollment, secondary, female (% net)\",\"SE.SEC.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"90.75925\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"101.10\",\"\","));
	  //mapDriver.withInput(new Object(), new Text("\"Country Name\",\"Country Code\",\"Indicator Name\",\"Indicator Code\",\"1960\",\"1961\",\"1962\",\"1963\",\"1964\",\"1965\",\"1966\",\"1967\",\"1968\",\"1969\",\"1970\",\"1971\",\"1972\",\"1973\",\"1974\",\"1975\",\"1976\",\"1977\",\"1978\",\"1979\",\"1980\",\"1981\",\"1982\",\"1983\",\"1984\",\"1985\",\"1986\",\"1987\",\"1988\",\"1989\",\"1990\",\"1991\",\"1992\",\"1993\",\"1994\",\"1995\",\"1996\",\"1997\",\"1998\",\"1999\",\"2000\",\"2001\",\"2002\",\"2003\",\"2004\",\"2005\",\"2006\",\"2007\",\"2008\",\"2009\",\"2010\",\"2011\",\"2012\",\"2013\",\"2014\",\"2015\",\"2016\","));
    /*
     * The expected output is "cat 1", "cat 1", and "dog 1".
     */
    mapDriver.withOutput(new Text("United States"), new DoubleWritable(101.10));

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
    values.add(new DoubleWritable(14.8));

    /*
     * For this test, the reducer's input will be "cat 1 1".
     */
    reduceDriver.withInput(new Text("United States"), values);

    /*
     * The expected output is "cat 2"
     */
    reduceDriver.withOutput(new Text("United States"), true);

    /*
     * Run the test.
     */
    reduceDriver.runTest();
  }

//  /*
//   * Test the mapper and reducer working together.
//   */
//  @Test
//  public void testMapReduce() {
//
//    /*
//     * For this test, the mapper's input will be "1 cat cat dog" 
//     */
//    mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
//
//    /*
//     * The expected output (from the reducer) is "cat 2", "dog 1". 
//     */
//    mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
//    mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
//
//    /*
//     * Run the test.
//     */
//    mapReduceDriver.runTest();
//  }
}