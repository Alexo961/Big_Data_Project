package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondJobSecondMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String byYearSector = line.split("\t")[0];
		String mapValues = line.split("\t")[1]; //vol_var_quot
		
		context.write(new Text(byYearSector), new Text(mapValues));
	}

}
