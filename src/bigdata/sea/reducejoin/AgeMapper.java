package bigdata.sea.reducejoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AgeMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Map<String, Integer> usersMap;

	public AgeMapper() {
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		usersMap = new HashMap<String, Integer>();
		Configuration conf = context.getConfiguration();
		String fileName = conf.get(AverageAgeApplication.FRIENDS);

		FileSystem fileSystem = FileSystem.get(conf);
		Path filePath = new Path(fileName);
		FileStatus[] fileSystemStatus = fileSystem.listStatus(filePath);
		for (FileStatus status : fileSystemStatus) {
			Path userDataPath = status.getPath();
			BufferedReader br = new BufferedReader(
					new InputStreamReader(fileSystem.open(userDataPath)));

			String profile = null;
			do {
				profile = br.readLine();
				if (profile != null) {
					String[] userData = profile.split(",");
					if (userData.length > 1) {
						usersMap.put(userData[0].trim(),
								(Calendar.getInstance().get(
										Calendar.YEAR)
								- Integer.parseInt(userData[9].split("/")[2])));
					} else {
						usersMap.put(userData[0].trim(), null);
					}
				}
			} while (profile != null);
		}

	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		String profile[] = value.toString().split("\t");
		Text user = new Text(profile[0]);
		StringBuilder friendsAges = new StringBuilder();
		if (profile.length == 2) {
			for (String friend : profile[1].split(",")) {
				Integer friendsAge = usersMap.get(friend);
				if (friendsAge != null)
					friendsAges.append(friendsAge + ",");
			}
			context.write(user, new Text(
					friendsAges.substring(0, friendsAges.length() - 1)));
		}
	}

}
