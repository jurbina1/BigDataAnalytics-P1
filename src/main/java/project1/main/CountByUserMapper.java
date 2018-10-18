package project1.main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

public class CountByUserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	JSONObject obj = new JSONObject(value.toString());
    	String usrId =  obj.getJSONObject("user").getString("id_str");

        context.write(new Text(usrId), new IntWritable(1));
    }

}
