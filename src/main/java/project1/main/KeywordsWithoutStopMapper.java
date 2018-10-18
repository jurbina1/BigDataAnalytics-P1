package project1.main;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.JSONObject;

public class KeywordsWithoutStopMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	JSONObject obj = new JSONObject(value.toString());
    	
    	String[] stop = {"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "he’d", "he’ll", "he’s", "her", "here", "here’s", "hers", "herself", "him", "himself", "his", "how", "how’s", "I", "I’d", "I’ll", "I’m", "I’ve", "if", "in", "into", "is", "it", "it’s", "its", "itself", "let’s", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she’d", "she’ll", "she’s", "should", "so", "some", "such", "than", "that", "that’s", "the", "their", "theirs", "them", "themselves", "then", "there", "there’s", "these", "they", "they’d", "they’ll", "they’re", "they’ve", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "we’d", "we’ll", "we’re", "we’ve", "were", "what", "what’s", "when", "when’s", "where", "where’s", "which", "while", "who", "who’s", "whom", "why", "why’s", "with", "would", "you", "you’d", "you’ll", "you’re", "you’ve", "your", "yours", "yourself", "yourselves"};
    	
    	String str = obj.getJSONObject("extended_tweet").getString("full_text"); 
    	str = str.replaceAll("[^A-Za-z0-9]", " ");
    	String[] words = str.split("\\s");
    	
    	for(String word : words) {
    		if (!ArrayUtils.contains(stop, word.toLowerCase()) && !word.toLowerCase().equals("")) {
    			context.write(new Text(word.toLowerCase()), new IntWritable(1));
    		}
    	}
    }
}
