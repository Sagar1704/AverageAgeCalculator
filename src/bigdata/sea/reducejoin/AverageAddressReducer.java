package bigdata.sea.reducejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageAddressReducer extends Reducer<Text, Text, Text, Text> {
	private ArrayList<User> users;

	public AverageAddressReducer() {
		users = new ArrayList<User>();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		String address = "";
		double averageAge = 0.0;
		for (Text value : values) {
			if (value.toString().contains(",")) {
				address = value.toString();
			} else {
				averageAge = Double.parseDouble(value.toString());
			}
		}
		users.add(new User(key.toString(), address, averageAge));
	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Collections.sort(users);
		int count = 0;
		for (User user : users) {
			if (count == 20)
				break;
			context.write(new Text(user.toString()), null);
			count++;
		}
	}
}
