package bigdata.sea.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageAgeApplication extends Configured implements Tool {
	public static final String FRIENDS = "friends";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new AverageAgeApplication(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println(
					"usage: [userData] [input] [intermediate_output] [output]");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		String[] otherArguments = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		conf.set(FRIENDS, otherArguments[0]);

		Job job1 = new Job(conf, "averageAge");

		job1.setJarByClass(AverageAgeApplication.class);
		job1.setMapperClass(AgeMapper.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(otherArguments[1]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArguments[2]));
		
		if (job1.waitForCompletion(true)) {
			Job job2 = new Job(conf, "averageAgeAddress");

			job2.setJarByClass(AverageAgeApplication.class);
			job2.setReducerClass(AverageAddressReducer.class);

			MultipleInputs.addInputPath(job2, new Path(otherArguments[2]),
					TextInputFormat.class, AverageAgeMapper.class);
			MultipleInputs.addInputPath(job2, new Path(otherArguments[0]),
					TextInputFormat.class, AddressMapper.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			job2.setOutputFormatClass(TextOutputFormat.class);

			FileOutputFormat.setOutputPath(job2, new Path(otherArguments[3]));
			if(job2.waitForCompletion(true))
				return 1;
		}
		return 0;
	}

}
