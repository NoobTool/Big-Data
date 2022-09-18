package edu.rmit.cosc2367.s3853868.WordLengthCategorizer;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
public class WordCountNotPreserved extends Configured implements Tool {
	public static final Log log = LogFactory.getLog(WordCountNotPreserved.class);
	
	//Map Class
	   static public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	   
	      @Override
	      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
	    	  HashMap<String,Integer> wordMap = new HashMap<String,Integer>();
	    	  
	    	  log.info("The mapper task of Ram Rattan Goyal , s3853868");
	    	  //Split line into tokens
	    	  for (String token : text.toString().split("\\s+")) {
	            if(wordMap.containsKey(token))
	            	wordMap.put(token,wordMap.get(token)+1);
	            else
	            	wordMap.put(token,1);
	            
	            // The emit function which takes the <k,v> from the hashmap and emits it.
	            for(HashMap.Entry<String,Integer> entry: wordMap.entrySet())
	            	context.write(new Text(entry.getKey()),new LongWritable(entry.getValue()));
	         }
	      }
	   }

	   //Reducer
	   static public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	      private LongWritable total = new LongWritable();

	      @Override
	      protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
	            throws IOException, InterruptedException {
	    	  
	    	 log.info("The reducer task of Ram Rattan Goyal , s3853868"); 
	         long n = 0;
	         //Calculate sum of counts
	         for (LongWritable count : counts)
	            n += count.get();
	         total.set(n);
	 
	        
	         context.write(token, total);
	      }
	   }

	   public int run(String[] args) throws Exception {
	      Configuration configuration = getConf();
	      configuration.set("mapreduce.job.jar", args[2]);
	      
	      //Initialising Map Reduce Job
	      Job job = new Job(configuration, "Word Count Not Preserved");
	      
	      //Set Map Reduce main jobconf class
	      job.setJarByClass(WordCountNotPreserved.class);
	      
	      //Set Mapper class
	      job.setMapperClass(WordCountMapper.class);
	      
	      //set Reducer class
	      job.setReducerClass(WordCountReducer.class);

	      //set Input Format
	      job.setInputFormatClass(TextInputFormat.class);
	      
	      //set Output Format
	      job.setOutputFormatClass(TextOutputFormat.class);

	      //set Output key class
	      job.setOutputKeyClass(Text.class);
	      
	      //set Output value class
	      job.setOutputValueClass(LongWritable.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args [1]));



	      return job.waitForCompletion(true) ? 0 : -1;
	   }

	   public static void main(String[] args) throws Exception {
	      System.exit(ToolRunner.run(new WordCountNotPreserved(), args));
	   }
	}
