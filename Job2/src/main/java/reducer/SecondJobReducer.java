package reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import objects.ActionObject;
import objects.SectorObject;
import objects.StockObject;
import support.Supports;

public class SecondJobReducer extends Reducer<Text, Text, Text, Text> {

	private StockObject stock;
	private SectorObject sect;
	private ActionObject action;
	
	@Override
	public void reduce(Text key, Iterable<Text> lines, Context context)
			throws IOException, InterruptedException {
		
		for (Text line : lines) {
			action = Supports.textToActionObject(line);
		}
		
	}
}
