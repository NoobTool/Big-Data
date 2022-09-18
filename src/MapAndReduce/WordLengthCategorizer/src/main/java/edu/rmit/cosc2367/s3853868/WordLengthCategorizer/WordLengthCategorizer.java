package edu.rmit.cosc2367.s3853868.WordLengthCategorizer;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class WordLengthCategorizer extends Configured implements Tool {
	public static final Log log = LogFactory.getLog(WordLengthCategorizer.class);
	
	//Map Class
	   static public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	   
		  final private static LongWritable ONE = new LongWritable(1);
	      private Text tokenValue = new Text();

	      @Override
	      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
	    	  log.info("The mapper task of Ram Rattan Goyal , s3853868");
	    	  //Split line into tokens
	    	  for (String token : text.toString().split("\\s+")) {
	    		  
	    		// Check the length of each token and assign a category  
	            if(token.length()>=1 && token.length()<=4)
	            	tokenValue.set("Small");
	            else if(token.length()>=5 && token.length()<=7)
	            	tokenValue.set("Medium");
	            else if(token.length()>=8 && token.length()<=10)
	            	tokenValue.set("Large");
	            else
	            	tokenValue.set("Extra-Large");
	            
	            context.write(tokenValue, ONE);
	         }
	      }
	   }

	   //Reducer
	   static public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	      private LongWritable total = new LongWritable();

	      @Override
	      protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
	            throws IOException, InterruptedException {
	         long n = 0;
	         log.info("The reducer task of Ram Rattan Goyal , s3853868");
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
	      Job job = new Job(configuration, "Word Length Categorizer");
	      
	      //Set Map Reduce main jobconf class
	      job.setJarByClass(WordLengthCategorizer.class);
	      
	      //Set Mapper class
	      job.setMapperClass(WordCountMapper.class);
	      
	     //Set Combiner class
	      job.setCombinerClass(WordCountReducer.class);
	      
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
	      System.exit(ToolRunner.run(new WordLengthCategorizer(), args));
	   }
	}
