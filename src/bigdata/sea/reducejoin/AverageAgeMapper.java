package bigdata.sea.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageAgeMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		String[] userFriendsAges = value.toString().split("\t");
		int count = 0;
		double ageSum = 0;
		String[] ages = userFriendsAges[1].split(",");
		for (String age : ages) {
			ageSum += Integer.parseInt(age);
			count++;
		}

		context.write(new Text(userFriendsAges[0]), new Text("" + (ageSum / count)));
	}
}
