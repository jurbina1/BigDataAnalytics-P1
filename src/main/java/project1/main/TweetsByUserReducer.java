package project1.main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class TweetsByUserReducer extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String list= "";

        for (Text value : values){
            list = list+value+" ";
        }
        //Logger loggerThing = LogManager.getRootLogger();

        // DEBUG
        context.write(key, new Text(list));
    }
}
