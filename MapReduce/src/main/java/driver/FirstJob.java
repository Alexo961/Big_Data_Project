package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapper.ActionMapper;
import reducer.FirstJobFirstReducer;

public class FirstJob {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance( new Configuration(),"First Job");
		
        job.setJarByClass(FirstJob.class);
		
		job.setMapperClass(ActionMapper.class);
		job.setReducerClass(FirstJobFirstReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}
