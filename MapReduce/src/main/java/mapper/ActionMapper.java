package mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import objects.StockObject;

public class ActionMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text ticker;
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		try {
		String line = value.toString();
		List<String> list = Arrays.asList(line.split(","));
		String tickerString = list.get(0);
		ticker = new Text(tickerString);
		context.write(ticker, value);
		}
		
		catch(Exception exc) {
			System.out.println("BAD LINE SKIPPED");
		}
		
	}
}
