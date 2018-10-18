package project1.main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public class TweetsByKeywordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable value : values ){
            count++;
        }

        //Logger logger = LogManager.getRootLogger();

        context.write(key, new IntWritable(count));
    }
}
