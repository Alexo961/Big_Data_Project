package Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
    Text word = new Text(ticker);
    List<String> list = Arrays.asList(line.split(","));
    
    if (list.size() == 8) {
    	if(list.get(0).toString().equals("ticker"))
    		System.out.println("SALTO_PRIMA_RIGA");
    	context.write(word, new Text(line));
    }
    else {
    	System.out.println("SALTO ELEMENTO SPURIO");
    }
    	
	}
	
}
