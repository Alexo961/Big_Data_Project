package Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import Supports.SupportObject;
import Supports.StockObject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper
	extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
	String line = value.toString();
	

	String ticker = line.split(",")[0];
    Text word = new Text();
    word.set(ticker);
    List<String> list = Arrays.asList(line.split(","));
    
    if (list.size() == 8) {
    	context.write(word, new Text(line));
    }
    else {
    	System.out.println("SALTO ELEMENTO SPURIO");
    }
    	
	}
	
}
