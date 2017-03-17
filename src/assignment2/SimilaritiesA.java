package assignment2;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import assignment2.SimilaritiesB.Reduce;


public class SimilaritiesA extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new SimilaritiesA(), args);
	      
	      System.exit(res);
	   }
	
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "SimiliratiesA");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ","); //We use ";" as a delimitor in the output file instead of tab
	      job.setJarByClass(SimilaritiesA.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("input/pg100-prepro.csv")); 
	      Path outputPath = new Path("output/Assignment2/SimilaritiesA/");
	      FileOutputFormat.setOutputPath(job, outputPath);
	      FileSystem hdfs = FileSystem.get(getConf());
		  if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		  }
		  
		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      System.out.println("Total number of comparisons : "+Reduce.numberComparisons);
	      System.out.println("Number of similar docs : "+Reduce.similarDocs);
	      
	      return 0;
	   }
	

	 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		 private Integer totalLines = 0;
		 
		 protected void setup(Context context) throws IOException, InterruptedException { 
			 //This function is called once at the beginning  of the mapping process
			 //It is reading the file outputed previously by the preprocessing file
			 //And is putting each docID and sentences of the file into a TreeMap
			 File inputFile = new File("/home/cloudera/workspace/Antoine_Pupin/input/pg100-prepro.csv");
	       	 BufferedReader read = new BufferedReader(new FileReader(inputFile));
	       	 while (read.readLine() != null){
	       		 totalLines++; //Increment the total number of line counter
	       	 }
	       	 read.close();
	       }
		 
	     @Override
	     public void map(LongWritable key, Text value, Context context)
	             throws IOException, InterruptedException {
	    	 
	    	 Integer lineNumber = Integer.parseInt(value.toString().split(";")[0]); //Get the number of the line (in the file)
	    	 String sentence = value.toString().split(";")[1]; // Get the full sentence
	    	 //Create a loop on the docId in order to get a pair of keys in the key of the context
	    	 for(Integer k=0;k<totalLines;k++){
	    	 	if(k!=lineNumber){
	    	 		//To make sure the key will be identical for a same pair of docId, we use min and max
	    	 		Integer minId = Math.min(k, lineNumber);
	    	 		Integer maxId = Math.max(k, lineNumber);
	    	 		//Then we concatenate both the docIDs
	    	 		String newKey = "("+Integer.toString(minId)+","+Integer.toString(maxId)+")";
	    	 		context.write(new Text(newKey), new Text(sentence) );
	    	 	}
	    	 }
	       }
	   }
	 
	 public static class Reduce extends Reducer<Text, Text, Text, Text> {
		 private static Integer numberComparisons = 0;
		 private static Integer similarDocs = 0;
		 
		 @Override
	     public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	         //Initialization of the variables
	    	 HashSet<String> hashset = new HashSet<String>();
	    	 float tot = 0;
	    	 float sim = 0;
	    	 float similarity = 0;
	    	 //The input of the reducer is a single key with two values in our case, which are the full sentences for both documents
	    	 //We create a loop on it in order to compute the similarity
	    	 for (Text value : values){
	    		 for (String word : value.toString().split(",")){
	    			 if(hashset.contains(word)){
	    				 sim++;
	    			 }
	    			 else {
	    				 hashset.add(word);
	    			 }
	    		 }
	    	 }
	    	 
	    	 tot = hashset.size();
	    	 
	    	 similarity = sim/tot;
	    	 //If the similarity is greater than 0,8 (which in this case is our treshold) we write the pair of docIds in the context with the associated similarity
	    	 if(similarity>0.8){
    			 context.write(key, new Text(Float.toString(similarity)));
    			 similarDocs++; /// We increment the counter for similar documents
    		 }
	    	 numberComparisons++; //We increment the counter for the number of comparisons done by the algorithm
	      }
	}
}
