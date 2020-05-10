package reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import support.*;

public class JoinReducer extends  Reducer<TextPair, Text, Text, Text> {

	@Override
	public void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		 Iterator<Text> iterator = values.iterator();
		Text nodeId = new Text(iterator.next());
		while (iterator.hasNext()) {
			Text node = iterator.next();
			Text outValue = new Text(nodeId.toString() + "," + node.toString());
			context.write(key.getFirst(), outValue);
		}
	}
}

