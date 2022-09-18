package edu.rmit.cosc2367.s3853868.WordPairCounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

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

public class WordPairCount extends Configured implements Tool {
	public static final Log log = LogFactory.getLog(WordPairCount.class);
	
	//Map Class
	   static public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	   
		  final private static LongWritable ONE = new LongWritable(1);
	      private Text tokenValue = new Text();
	      private Text word1 = new Text();
	      private Text word2 = new Text();

	      @Override
	      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
	    	  
	    	  // String tokenizing using newline
	    	  StringTokenizer newlineTokenizer = new StringTokenizer(text.toString(),"\n");
	    	  ArrayList<String> wordList = new ArrayList<String>();
	    	  int wordIndex;
	    	  
	    	  while(newlineTokenizer.hasMoreTokens()) {
	    		  String currentLine = newlineTokenizer.nextToken();
	    		  
	    		  // Storing all the words in the line in an ArrayList
	    		  StringTokenizer spaceTokenizer = new StringTokenizer(currentLine," ");
	    		  while(spaceTokenizer.hasMoreTokens())
	    			  wordList.add(spaceTokenizer.nextToken());
	    		  
	    		  // Going through the line to check for co-occurrence of words
	    		  StringTokenizer spaceTokenizer2 = new StringTokenizer(currentLine," ");
	    		  while(spaceTokenizer2.hasMoreTokens()) {
	    			  String currentWord = spaceTokenizer2.nextToken();
	    			  wordIndex = wordList.indexOf(currentWord);
	    			  log.info("\n\nCurrent word is "+currentWord);
	    			  
	    			  // Looping in the range of 3 before and after words
		    		  for(int i=wordIndex-3;i<=wordIndex+3;i++) {
		    			  try {
		    				  
		    				  // If the same word occurs, continue
		    				  if(i==wordIndex)
		    					  continue;
		    				  tokenValue.set("("+currentWord+","+wordList.get(i)+")");  
		    				  context.write(tokenValue, ONE);
		    			  }catch(IndexOutOfBoundsException e) {
		    				  continue;
		    			  }
		    		  }
	    		  }
	    		  
	    		  // Clearing wordList for other lines
	    		  wordList.clear();
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
	         // Calculate sum of counts
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
	      job.setJarByClass(WordPairCount.class);
	      
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
	      System.exit(ToolRunner.run(new WordPairCount(), args));
	   }
	}
