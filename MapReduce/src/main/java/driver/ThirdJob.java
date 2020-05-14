package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import mapper.Job2Map2Mapper;

import reducer.Job2Redu2Reducer;

public class ThirdJob {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance( new Configuration(),"Third Job");
		
        job.setJarByClass(ThirdJob.class);
		
		job.setMapperClass(Job3Map3Mapper.class);
		job.setReducerClass(Job3Redu3Reducer.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}
