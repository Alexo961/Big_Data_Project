package mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import objects.StockObject;

public class Job1_2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text ticker;

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		    
			String line = value.toString();
			String[] str = line.split(",");
			if(str.length == 9) {
			String settore_data = str[1]+str[8];
			 Text chiave = new Text(settore_data);
			 context.write(chiave, value);
			}
			
			else {
				
				System.out.println("Riga spuria");
			}
		}

		

	}
}
