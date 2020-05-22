package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import objects.ActionObject;
import objects.JoinObject;
import support.ObjectSupports;

public class ThirdJobFirstMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString().split("\t")[1];
		Text realValue = new Text(line);

		ActionObject ao = ObjectSupports.textToActionObject(realValue);

		if (ao != null && ao instanceof JoinObject) {
			JoinObject jo = (JoinObject) ao;
			Integer year = jo.getStock().getDate().getYear();
			if(year >=2016) {
				String name = jo.getSector().getName();
				String ticker = jo.getSector().getTicker();
				String byYearNameTicker = year + "_" + name + "_" + ticker;
				context.write(new Text(byYearNameTicker), realValue);
			}
		}
	}
}
