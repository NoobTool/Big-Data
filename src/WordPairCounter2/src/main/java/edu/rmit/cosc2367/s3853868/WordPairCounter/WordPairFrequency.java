package edu.rmit.cosc2367.s3853868.WordPairCounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class WordPairFrequency extends Configured implements Tool {
	public static final Log log = LogFactory.getLog(WordPairFrequency.class);
	
	//Map Class
	   static public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	   
	      private Text tokenValue = new Text();
	      private Text wordFreq = new Text();
	      private HashMap<String,String> wordMap = new HashMap<String,String>();
	      private HashMap<String,String> universeMap = new HashMap<String,String>();
	      private int lineLength=0;
	      @Override
	      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
	    	  
	    	  // Tokenizing with newline characters
	    	  StringTokenizer newlineTokenizer = new StringTokenizer(text.toString(),"\n");
	    	  ArrayList<String> wordList = new ArrayList<String>();
	    	  int wordIndex;
	    	  int lineNumber=0;
	    	  
	    	  
	    	  while(newlineTokenizer.hasMoreTokens()) {
	    		  String currentLine = newlineTokenizer.nextToken();
	    		  
	    		  StringTokenizer spaceTokenizer = new StringTokenizer(currentLine," ");
	    		  // Generating a list of all words in the line
	    		  while(spaceTokenizer.hasMoreTokens())
	    			  wordList.add(spaceTokenizer.nextToken());
	    		  
	    		  lineLength+=wordList.size();
	    		  lineNumber+=1;
	    		  
	    		  // Going through the line to check for co-occurrence of words
	    		  StringTokenizer spaceTokenizer2 = new StringTokenizer(currentLine," ");
	    		  while(spaceTokenizer2.hasMoreTokens()) {
	    			  String currentWord = spaceTokenizer2.nextToken();
	    			  wordIndex = wordList.indexOf(currentWord);
	    			  
		    		  for(int i=wordIndex-3;i<=wordIndex+3;i++) {
		    			  try {
		    				  if(i==wordIndex)
		    					  continue;
		    				  
		    				  // Making the pair a key
		    				  String key = "("+currentWord+","+wordList.get(i)+")";
		    				  if(wordMap.containsKey(key)) {
		    					  String[] value = wordMap.get(key).split("/");
		    					  log.info("key and value are "+key+","+value[0]);
		    					  
		    					  // Additions when a co-occured word is found
		    					  wordMap.put(key, (Integer.valueOf(value[0])+1)+"/"+
		    					  wordList.size());
//		    							  (Integer.valueOf(value[1])+lineLength));
		    							  
		    				  }
		    				  else
		    					  wordMap.put(key, "1/"+wordList.size());
		    					  
//		    				  context.write(tokenValue, );
		    			  }catch(IndexOutOfBoundsException e) {
		    				  continue;
		    			  }
		    		  }
	    		  }
	    		  
	    		  log.info("\n\n\n\n\nLine Length is"+lineLength+" This is the hashmap"+wordMap);
	    		  for(String key: wordMap.keySet()) {
	    			  if(universeMap.containsKey(key)) {
	    				  String [] wordMapFreq = wordMap.get(key).split("/");
	    				  String[] universeMapFreq = universeMap.get(key).split("/");
	    				  String value = (Integer.valueOf(wordMapFreq[0])
	    						  +Integer.valueOf(universeMapFreq[0]))+"/"+
	    						  (Integer.valueOf(wordMapFreq[1])+
	    								  Integer.valueOf(universeMapFreq[1]));
	    				  
	    				  universeMap.put(key,value);
	    			  }
	    			  
	    			  else
	    				  universeMap.put(key, wordMap.get(key));
	    		  }
	    		  wordList.clear();
	    		  wordMap.clear();
	    	  }
	    	  
	    	  for (HashMap.Entry<String, String> entry : universeMap.entrySet()) {
	    		  tokenValue.set(entry.getKey());
	    		  wordFreq.set(entry.getValue());
	    		  context.write(tokenValue, wordFreq);
	    	  }
	      }
	   }

	   //Reducer
	   static public class WordCountReducer extends Reducer<Text, Text, Text, Text> {
	      private Text total = new Text();

	      @Override
	      protected void reduce(Text token, Iterable<Text> counts, Context context)
	            throws IOException, InterruptedException {
	         String n="";
	         int max=0,max2=0 ;
	         
	         //Calculate sum of counts
	         for (Text count : counts) {
	        	 log.info("\n\n\nThe token and count are "+token+", "+count.toString());
//	        	 log.info("\n\n This is the string "+count.toString()+"\n\n");
	        	 if(n=="") {
	        		 n=count.toString();
	        		 max= Integer.valueOf(count.toString().split("/")[0]);
	        		 max2= Integer.valueOf(count.toString().split("/")[1]);
	        	 }
	        	 else {
	        		 
	        		 // Adding numerator and denominator to calculate the word frequency
	        		 String[] c = count.toString().split("/");
	        		 if(max<Integer.valueOf(c[0]))
	        			 max = Integer.valueOf(c[0]);
	        		 if(max2<Integer.valueOf(c[1]))
	        			 max2 = Integer.valueOf(c[1]);
//	        		 n = max+"/"+(Integer.valueOf(n.split("/")[1])+Integer.valueOf(c[1]));
	        		 n = max+"/"+max2;
	        	 }
	         }
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
	      job.setJarByClass(WordPairFrequency.class);
	      
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
	      job.setOutputValueClass(Text.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args [1]));



	      return job.waitForCompletion(true) ? 0 : -1;
	   }

	   public static void main(String[] args) throws Exception {
	      System.exit(ToolRunner.run(new WordPairFrequency(), args));
	   }
	}
