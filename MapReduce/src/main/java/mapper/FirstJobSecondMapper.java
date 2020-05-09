package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstJobSecondMapper extends Mapper<LongWritable, Text, Text, Text> {

}
