package bigdata.sea.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AddressMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		String[] profile = value.toString().split(",");
		context.write(new Text(profile[0]),
				new Text(profile[3] + ", " + profile[4] + ", " + profile[5]));
	}
}
