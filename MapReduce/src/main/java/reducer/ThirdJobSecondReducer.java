package reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdJobSecondReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		double variationSum = 0;
		double count = 0;

		for (Text value : values) {
			if (value != null) {
				String varStr = value.toString();
				double variation = Double.parseDouble(varStr);
				variationSum += variation;
				count++;

			}
		}
		String keyStr = key.toString();
		String year = keyStr.split("_")[0];
		String name = keyStr.split("_")[1];
		double variationAvg = variationSum / count;
		long variationRound = Math.round(variationAvg);
		String variationStr = Long.toString(variationRound);
		String year_var = year + "_" + variationStr;
		
		context.write(new Text(name), new Text(year_var));
	}
}
