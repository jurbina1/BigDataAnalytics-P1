package project1.main;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public class RepliesPerMessageReducer extends Reducer<Text, Text, Text, Text> {

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
