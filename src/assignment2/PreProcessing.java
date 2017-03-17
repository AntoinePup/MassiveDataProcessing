package assignment2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.SortedMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PreProcessing extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new PreProcessing(), args);
	      
	      System.exit(res);
	   }
	
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "PreProcessing");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); //We use ";" as a delimitor in the output file instead of tab
	      job.setJarByClass(PreProcessing.class);
	      job.setOutputKeyClass(LongWritable.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("input/pg100.txt")); 
	      Path outputPath = new Path("output/Assignment2/Preprocessing/");
	      FileOutputFormat.setOutputPath(job, outputPath);
	      FileSystem hdfs = FileSystem.get(getConf());
		  if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		  }
		  
		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      return 0;
	   }

	 public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
	      private Text word = new Text();
	      private Set<String> stopWords = new HashSet<String>();
	      private static LongWritable sentenceCounter = new LongWritable(0);
	      
	      
	      
	      protected void setup(Context context) throws IOException, InterruptedException { 
	    	  	 //We read the file which contains the list of stopwords
		     	 File inputFile = new File("/home/cloudera/workspace/Antoine_Pupin/input/stopwords2.txt");
		       	 BufferedReader read = new BufferedReader(new FileReader(inputFile));
		       	 
		       	 //First we read the file and we add each words into a list
		       	 String stopword = null;
		       	 while ((stopword = read.readLine()) != null){
		       		 stopWords.add(stopword);
		       	 }
		       	 read.close(); // Once we've been through the file, we close it
		  }
	      
	      
	     @Override
	     public void map(LongWritable key, Text value, Context context)
	             throws IOException, InterruptedException {
	    	 //Variable initialization
	    	 String sentence = new String();
	    	 
	       	 sentence = value.toString().replaceAll("[^0-9A-Za-z]"," "); // We replace avery characters which are not wanted by an empty space
	       	 //We check if the line is not empty
	       	 if(!sentence.isEmpty()){
	       		 //Then we read split the input document into sentences
		         for (String token: sentence.split("\\s+")){
		        	 //We check if the token is not empty and doesn't belongs to the stopwords list
		        	 if (!stopWords.contains(token) && !token.isEmpty()){
		        		 word.set(token);
		        		 context.write(sentenceCounter,word); //Write the sentence and docId in the context
			 	     }	        	 	
		           }
		           sentenceCounter.set(sentenceCounter.get()+1); //Increment the sentence counter
	       	 }
	       }
	   }
	 
	 public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
		 private  TreeMap <String, Integer> wordFrequencies = new TreeMap<String, Integer>();
		 private HashMap <String, Integer> counter = new HashMap<String, Integer>();
		 
		 protected void setup(Context context) throws IOException, InterruptedException { 
			 //Read the source file and raple every unwanted characters
			 //Than add each remaining words to the treemap in order to count the number of occurence of each words in the corpus
			 File inputFile = new File("/home/cloudera/workspace/Antoine_Pupin/input/pg100.txt");
	       	 BufferedReader read = new BufferedReader(new FileReader(inputFile));
	       	String line = null;
	       	 while ((line = read.readLine()) != null){
	       		 for ( String token : line.replaceAll("[^0-9A-Za-z]"," ").split(" ")){
	       			 if(!token.isEmpty()){
	       				if(!wordFrequencies.containsKey(token)){
			    			 wordFrequencies.put(token, 0);
			    		 }
			    		 else {
			    			 wordFrequencies.put(token,wordFrequencies.get(token)+1);
			    		 }
	       			 }
	       		 }
	       	 }
	       	 read.close();
	       }

		 @Override
	     public void reduce(LongWritable key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	         //Initialization of the variables
	    	 HashSet<String> hashset = new HashSet<String>();
	    	 SortedMap <Integer, String> sortedWords = new TreeMap<Integer, String>();
	    	 String sentence = new String();
	    	 
	    	 for (Text value : values){	 
	    		//We put every different words into the hashset in order to avoid doublons
	    		 hashset.add(value.toString());
	    	 }
	    	 //We put every words os the sentence in a SortedMap
	    	 for (String value : hashset){
	    		 sortedWords.put(wordFrequencies.get(value),value);
	    	 }
	    	 //Then we get the sorted (reverse) set of values of the sortedMap and we concatenate the words
	    	 for (String word : sortedWords.values()){
	    			 sentence += word+",";
	    	 }
	    	 //We remove the last comma and we add the key and sentence in the context
		     sentence = sentence.substring(0, sentence.length()-1);
		     
		     //We check if the sentence is not empty
		     if(sentence.length()>0){
		    	 //Increment the counter
		    	 if(!counter.containsKey("totalLines")){
		    		 counter.put("totalLines", 1);
		    	 }
		    	 else {
		    		 counter.put("totalLines",counter.get("totalLines")+1);
		    	 }
			     context.write(key, new Text(sentence));
		     }
	      }
		 
		 protected void cleanup(Context context) throws IOException, InterruptedException {
			 //This function is called when the reduce phase is over
			 //Is putting the value of the counter into a txt file
		       try{
		          Path pt=new Path("output/Assignment2/Preprocessing/Counter.txt");
		          FileSystem fs = FileSystem.get(new Configuration());
		          BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
		          String line1=counter.get("totalLines") + "\n";
		          br.write(line1);
		          br.close();
		    }catch(Exception e){
		            System.out.println("Exception caught");
		    }
		}
	}
	 
	 

}
