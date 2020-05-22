package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import objects.ActionObject;
import objects.JoinObject;
import support.ObjectSupports;

public class ThirdJobThirdMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		String name = line.split("\t")[0];
		String year_var = line.split("\t")[1];
		
		context.write(new Text(name), new Text(year_var));
	}
}
