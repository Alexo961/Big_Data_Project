package reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import objects.ActionObject;
import objects.SectorObject;
import objects.StockObject;
import support.ObjectSupports;

public class Test extends Reducer<Text, Text, Text, Text> {

	private static final int SECTOR_NUM_FIELDS = 5;
	private static final int STOCK_NUM_FIELDS = 8;
	private StockObject stock;
	private SectorObject sect;
	private ActionObject action;
	
	@Override
	public void reduce(Text key, Iterable<Text> lines, Context context)
			throws IOException, InterruptedException {
		
		for (Text line : lines) {
			action = ObjectSupports.textToActionObject(line);
			if (action == null)
				System.out.println("BAD REDUCE LINE");
			if (action.getNumFields() == SECTOR_NUM_FIELDS)
				sect = (SectorObject) action;
		}
		for (Text line : lines) {
			action = ObjectSupports.textToActionObject(line);
			if (action == null)
				System.out.println("BAD REDUCE LINE SKIPPED");
			if (action.getNumFields() == STOCK_NUM_FIELDS) {
				stock = (StockObject) action;
				
			}
		}
		
	}
}
