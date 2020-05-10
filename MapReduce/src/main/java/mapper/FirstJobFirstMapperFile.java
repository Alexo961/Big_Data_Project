
package mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import support.ObjectSupports;
import support.TextPair;

public class FirstJobFirstMapperFile extends Mapper<LongWritable, Text, TextPair, Text> {

	private static final int SECTOR_NUM_FIELDS = 5;
	private static final int TICKER_POSITION = 0;
	private static final int NAME_POSITION = 1;
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{	

		String valueString = value.toString();
		String[] SingleNodeData = valueString.split(",");
		List<String> list = new ArrayList<>();
		int i = 0;

		while (i < SECTOR_NUM_FIELDS) {
			if (i != TICKER_POSITION && i != NAME_POSITION) {
				list.add(SingleNodeData[i]);
			}
		}
		context.write(new TextPair(SingleNodeData[0], "0"), ObjectSupports.listToText(list));
		//This output collector exposes the API for emitting tuples from an IRichBolt. This is the core API for emitting tuples. For a simpler API, and a more restricted form of stream processing, see IBasicBolt and BasicOutputCollector.
	}
}

