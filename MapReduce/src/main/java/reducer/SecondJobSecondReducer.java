package reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondJobSecondReducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		//System.out.println("SecondReducer");
		//System.out.println(key);
		
		double variationsAvg = 0;
		double quotationsAvg = 0;
		double volumesAvg = 0;
		double variationsSum = 0;
		double quotationsSum = 0;
		double volumesSum = 0;
		int count = 0;
		
		for(Text value : values) {
			
			//System.out.println(value);
			if (value != null) {
				String line = value.toString();
				double volume = Double.parseDouble(line.split("_")[0]);
				double variation = Double.parseDouble(line.split("_")[1]);
				double quotation = Double.parseDouble(line.split("_")[2]);
				count++;
				volumesSum += volume;
				variationsSum += variation;
				quotationsSum += quotation;
			}
		}
		
		volumesAvg = (volumesSum / count);
		variationsAvg = (variationsSum / count);
		quotationsAvg = (quotationsSum / count);
		
		String result = "vol: " + volumesAvg + ", var: " + variationsAvg + ", quot: " + quotationsAvg;
		
		context.write(key, new Text(result));
	}

}
