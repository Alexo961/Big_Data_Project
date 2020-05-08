package Job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Mapper.Job1Mapper;
import Reducer.Job1Reducer;
import Supports.StockObject;

public class Job1 {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance( new Configuration(),"Job1");
		
        job.setJarByClass(Job1.class);
		
		job.setMapperClass(Job1Mapper.class);
		job.setReducerClass(Job1Reducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}

}
