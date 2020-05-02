package Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import Supports.SupportObject;
import Supports.StockObject;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Job1Mapper extends Mapper<LongWritable, Text, Text, StockObject> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
	String line = value.toString();
	String ticker = line.split(",")[0];
    Text word = new Text(ticker);
    List<String> list = Arrays.asList(line.split(","));
    StockObject so;
    
    so = SupportObject.transform(list);
    
    context.write(word, so);
 
 
 
	
	}
	
}
