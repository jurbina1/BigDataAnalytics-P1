import static spark.Spark.*;
import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.mapreduce.Job;
import org.json.*;

import spark.ModelAndView;
import spark.template.velocity.VelocityTemplateEngine;

public class Main {
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
            System.err.println("Usage: Main <input path> <output path>");
            System.exit(-1);
        }
        get("/", (req, res) -> "Hello World. Lets start!");
        
        get("/start", (req, res) -> {res.redirect("/1"); return null;});
        
        get("/1", (request, response) -> {
        	Job job = new Job();
            job.setJarByClass(Main.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            String outPath = args[1];
        	FileOutputFormat.setOutputPath(job, new Path(outPath+"1"));
        	job.setMapperClass(project1.main.TweetsByKeywordMapper.class);
            job.setReducerClass(project1.main.TweetsByKeywordReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.waitForCompletion(true);
            job.killJob();
        
        	File file = new File(outPath.substring(7)+"1"+"/part-r-00000"); 
            Scanner sc = new Scanner(file); 
            ArrayList<JSONObject> temp1 = new ArrayList<JSONObject>();
            while (sc.hasNextLine()) { 
            	String str = sc.nextLine();
            	String[] value = str.split("\\s");
            	JSONObject temp = new JSONObject();
            	temp.put("name", value[0]);
            	temp.put("value", Integer.parseInt(value[1]));
            	temp1.add(temp);
            }
            String result = new JSONObject().put("children", temp1).toString();
            try (FileWriter file2 = new FileWriter(outPath.substring(7)+".json", true)) {
    			file2.write(result+"\n");
    			System.out.println("Successfully Copied JSON Object to File...");
    		}
            response.redirect("/2");
            return "Done";//result;
        });
        get("/2", (request, response) -> {
        	Job job = new Job();
            job.setJarByClass(Main.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            String outPath = args[1];
        	FileOutputFormat.setOutputPath(job, new Path(outPath+"2"));
        	job.setMapperClass(project1.main.KeywordsWithoutStopMapper.class);
            job.setReducerClass(project1.main.KeywordsWithoutStopReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.waitForCompletion(true);
            job.killJob();
            
        	File file = new File(outPath.substring(7)+"2"+"/part-r-00000"); 
            Scanner sc = new Scanner(file); 
            ArrayList<JSONObject> temp1 = new ArrayList<JSONObject>();
            int count = 0;
            while (sc.hasNextLine() && count <20900) {//to view some actual words
            	sc.nextLine();
            	count++;
            }
            count=0;
            while (sc.hasNextLine()&& count<100) { 
            	String str = sc.nextLine();
            	String[] value = str.split("\\s");
            	JSONObject temp = new JSONObject();
            	temp.put("name", value[0]);
            	temp.put("value", Integer.parseInt(value[1]));
            	temp1.add(temp);
            	count++;
            }
            String result = new JSONObject().put("children", temp1).toString();
            try (FileWriter file2 = new FileWriter(outPath.substring(7)+".json", true)) {
    			file2.write(result+"\n");
    			System.out.println("Successfully Copied JSON Object to File...");

    		}
            response.redirect("/3");
            return "Done";//result;
        });
        get("/3", (request, response) -> {
        	Job job = new Job();
            job.setJarByClass(Main.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            String outPath = args[1];
        	FileOutputFormat.setOutputPath(job, new Path(outPath+"3"));
        	job.setMapperClass(project1.main.TweetsByScreenNameMapper.class);
            job.setReducerClass(project1.main.TweetsByScreenNameReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.waitForCompletion(true);
            job.killJob();
        
        	File file = new File(outPath.substring(7)+"3"+"/part-r-00000");  
            Scanner sc = new Scanner(file); 
            ArrayList<JSONObject> temp1 = new ArrayList<JSONObject>();
            int count=0;
            while (sc.hasNextLine()&& count<100) { 
            	String str = sc.nextLine();
            	String[] value = str.split("\\s");
            	JSONObject temp = new JSONObject();
            	temp.put("name", value[0]);
            	temp.put("value", Integer.parseInt(value[1]));
            	temp1.add(temp);
            	count++;
            }
            String result = new JSONObject().put("children", temp1).toString();
            try (FileWriter file2 = new FileWriter(outPath.substring(7)+".json", true)) {
    			file2.write(result+"\n");
    			System.out.println("Successfully Copied JSON Object to File...");
    		}
            response.redirect("/4");
            return "Done";//result;
        });
        get("/4", (request, response) -> {
        	Job job = new Job();
            job.setJarByClass(Main.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            String outPath = args[1];
        	FileOutputFormat.setOutputPath(job, new Path(outPath+"4"));
        	job.setMapperClass(project1.main.RepliesPerMessageMapper.class);
            job.setReducerClass(project1.main.RepliesPerMessageReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.waitForCompletion(true);
            job.killJob();
        
        	File file = new File(outPath.substring(7)+"4"+"/part-r-00000");  
            Scanner sc = new Scanner(file); 
            ArrayList<JSONObject> temp1 = new ArrayList<JSONObject>();
            int count=0;
            while (sc.hasNextLine()&& count<100) { 
            	String str = sc.nextLine();
            	String[] value = str.split("\\s");
            	JSONObject jsonString = new JSONObject();
            	ArrayList<String> arr = new ArrayList<String>();
            	for (int i =1; i< value.length; i++) {
            		arr.add(value[i]);
            	}	
            	jsonString.put(value[0], arr);
            	JSONObject temp = new JSONObject();
            	temp.put("name", value[0]);
            	temp.put("value", arr.size());
            	temp1.add(temp);
            	count++;
            }
            String result = new JSONObject().put("children", temp1).toString();
            try (FileWriter file2 = new FileWriter(outPath.substring(7)+".json", true)) {
    			file2.write(result+"\n");
    			System.out.println("Successfully Copied JSON Object to File...");
    		}
            response.redirect("/5");
            return "Done";//result;
        });
        get("/5", (request, response) -> {
        	Job job = new Job();
            job.setJarByClass(Main.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            String outPath = args[1];
        	FileOutputFormat.setOutputPath(job, new Path(outPath+"5"));
        	job.setMapperClass(project1.main.TweetsByUserMapper.class);
            job.setReducerClass(project1.main.TweetsByUserReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.waitForCompletion(true);
            job.killJob();
        
        	File file = new File(outPath.substring(7)+"5"+"/part-r-00000"); 
            Scanner sc = new Scanner(file); 
            ArrayList<JSONObject> temp1 = new ArrayList<JSONObject>();
            int count = 0;
            while (sc.hasNextLine()&& count<100) { 
            	String str = sc.nextLine();
            	String[] value = str.split("\\s");
            	JSONObject jsonString = new JSONObject();
            	ArrayList<String> arr = new ArrayList<String>();
            	for (int i =1; i< value.length; i++) {
            		arr.add(value[i]);
            	}	
            	jsonString.put(value[0], arr);
            	JSONObject temp = new JSONObject();
            	temp.put("name", value[0]);
            	temp.put("value", arr.size());
            	temp1.add(temp);
            	count++;
            }
            String result = new JSONObject().put("children", temp1).toString();
            try (FileWriter file2 = new FileWriter(outPath.substring(7)+".json", true)) {
    			file2.write(result+"\n");
    			System.out.println("Successfully Copied JSON Object to File...");
    		}
            return "Everything is done and the JSON is in the path provided.";
        });
        
    }
}