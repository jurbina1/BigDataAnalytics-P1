package project1.main;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;

import java.io.IOException;

public class TweetsByKeywordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		JSONObject obj = new JSONObject(value.toString());
        // get the tweet
        String tweet = obj.getJSONObject("extended_tweet").getString("full_text");

        // now emit the following key-pair: keyword, 1
        if (tweet.toLowerCase().contains("measles")) {
        	context.write(new Text("Measles"), new IntWritable(1));
        }
        if(tweet.toLowerCase().contains("headache")) {
        	context.write(new Text("Headache"), new IntWritable(1));
        }
        if(tweet.toLowerCase().contains("ebola")) {
        	context.write(new Text("Ebola"), new IntWritable(1));
        }
        if(tweet.toLowerCase().contains("diarrhea")) {
        	context.write(new Text("Diarrhea"), new IntWritable(1));
        }
        if(tweet.toLowerCase().contains("zika")) {
        	context.write(new Text("Zika"), new IntWritable(1));
        }
        if(tweet.toLowerCase().contains("flu")) {
        	context.write(new Text("Flu"), new IntWritable(1));
        }
        if(tweet.toLowerCase().contains("trump")) {
        	context.write(new Text("Trump"), new IntWritable(1));
        }
    }
}
