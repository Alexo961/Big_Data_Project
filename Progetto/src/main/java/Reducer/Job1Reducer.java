package Reducer;


import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import Supports.OutputObject;

public class Job1Reducer extends
Reducer<Text, List<String>, Text, OutputObject > {
	
	public void reduce(Text key, Iterable<List<String>> values,
			Context context) throws IOException, InterruptedException {
   
}
 }
