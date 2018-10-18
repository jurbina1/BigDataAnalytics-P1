package project1.main;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;

import java.io.IOException;

public class RepliesPerMessageMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		JSONObject obj = new JSONObject(value.toString());
        // get the tweet
		String tweetId =  obj.getString("id_str");
		
		if (!obj.isNull("in_reply_to_status_id_str")) {
			String replyId = obj.getString("in_reply_to_status_id_str");
	        
	        // now emit the following key-pair: usrId, tweet
	        context.write(new Text(replyId), new Text(tweetId));
		}
		else {
			context.write(new Text("Not a reply"), new Text(tweetId));
		}
    }
}

