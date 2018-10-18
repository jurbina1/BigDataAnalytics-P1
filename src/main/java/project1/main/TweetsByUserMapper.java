package project1.main;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;

import java.io.IOException;

public class TweetsByUserMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		JSONObject obj = new JSONObject(value.toString());
        // get the tweet
		String usrId =  obj.getJSONObject("user").getString("id_str");
        String tweetId = obj.getString("id_str");

        // now emit the following key-pair: usrId, tweet
        context.write(new Text(usrId), new Text(tweetId));
    }
}
