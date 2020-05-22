package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdJobSecondMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String byYearName = line.split("\t")[0];
		String variation = line.split("\t")[1];
		
		context.write(new Text(byYearName), new Text(variation));
		
	}
}
