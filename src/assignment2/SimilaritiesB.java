package assignment2;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class SimilaritiesB extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new SimilaritiesB(), args);
	      
	      System.exit(res);
	   }
	
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "SimiliratiesB");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ","); //We use ";" as a delimitor in the output file instead of tab
	      job.setJarByClass(SimilaritiesB.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);
	      
	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("input/pg100-prepro.csv")); 
	      Path outputPath = new Path("output/Assignment2/SimilaritiesB/");
	      FileOutputFormat.setOutputPath(job, outputPath);
	      FileSystem hdfs = FileSystem.get(getConf());
		  if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		  }
		  
		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
//	      FileInputFormat.addInputPath(job, new Path(args[0]));
//	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      System.out.println("Total number of comparisons : "+Reduce.numberComparisons);
	      System.out.println("Number of similar docs : "+Reduce.similarDocs);
	      
	      return 0;
	   }
	

	 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		 
		 
	     @Override
	     public void map(LongWritable key, Text value, Context context)
	             throws IOException, InterruptedException {
	    	 
	    	 	Integer lineNumber = Integer.parseInt(value.toString().split(";")[0]); //Get the line number from the input
	    	 	
	    	 	String sentence = value.toString().split(";")[1]; //Get the full sentence from the input
	    	 	
	    	 	Integer numberWords = sentence.split(",").length; //Get the number of words in the sentence
	    	 	//We assume the Jaccard similarity threshold is equal to 0.8
	    	 	Integer prefixValue = numberWords - (int)(Math.round(0.8*numberWords))+1; //Compute the number of words to keep
	    	 	
	    	 	//We output the right number of words from the sentence
	    	 	for (Integer i=0;i<prefixValue;i++){
	    	 		String word = sentence.split(",")[i];
	    	 		context.write(new Text(word),new IntWritable(lineNumber));
	    	 	}
	       }
	   }
	 
	 public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		 private static Integer numberComparisons = 0;
		 private static Integer similarDocs = 0;
		 private HashSet <String> comparisons = new HashSet<String>();
		 private TreeMap <Integer, String> dictionnary = new TreeMap<Integer, String>();
		 
		 protected void setup(Context context) throws IOException, InterruptedException { 
			 //This function is used in order to store every sentences of the input file into a Treemap
			 String line = null;
	       	 Integer lineNumber = 0;
	       	 String sentence = null;
	       	 
			 File inputFile = new File("/home/cloudera/workspace/Antoine_Pupin/input/pg100-prepro.csv");
	       	 BufferedReader read = new BufferedReader(new FileReader(inputFile));
	       	 while ((line = read.readLine()) != null){
	       		 lineNumber = Integer.parseInt(line.split(";")[0]);
	       		 sentence = line.split(";")[1];
	       		dictionnary.put(lineNumber, sentence);
	       	 }
	       	 read.close();
	       }
		 
		 @Override
	     public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
			 
			//Initialization of the variables
	    	 float tot = 0;
	    	 float sim = 0;
	    	 float similarity = 0;
	    	 Integer docId1 = 0;
			 Integer docId2 = 0;
			 HashSet<Integer> hashsetV = new HashSet<Integer>();
			 
			 //First we store every values into a hashet in order to be able to iterate on it several times
			 for (IntWritable value : values){
				 hashsetV.add(value.get());
			 }
			 
			 //Then we create two loops in order to compare each docIds associated to a given word (which is our key in this cas).
			 for (Integer value : hashsetV){
				 docId1 = value;
				 for(Integer value2 : hashsetV){
					 docId2 = value2;
					 //In order to make sure we don't do twice the same comparison, we use the min and max of each docIds and we store it into the hashset comparisons
					 Integer minId = Math.min(docId1, docId2);
					 Integer maxId = Math.max(docId1, docId2);
					 //If the docIds are different and have not been already compared, we do the comparison
					 if(docId1 != docId2 && !comparisons.contains(minId+","+maxId)){
						 HashSet<String> hashset = new HashSet<String>();
						 tot = 0;
						 sim = 0;
						 similarity = 0;
						 //We compute the similarity
						 for(String elt : dictionnary.get(docId1).toString().split(",")){
							 hashset.add(elt);
						 }
						 for(String elt : dictionnary.get(docId2).toString().split(",")){
							 if(hashset.contains(elt)){
								 sim++;
							 }
							 else {
								 hashset.add(elt);
							 }
						 }
						 
						 tot = hashset.size();
			    		 
			    		 if(tot!=0){
			    			 similarity = sim/tot;
			    		 }
						 similarity = sim/tot; 
						 
						 String newVal = "("+docId1+","+docId2+")";
						 //If the wimilarity is greater than the treshold (0,8 in our case) we output the pair of docIds
						 if(similarity>0.8){
							 context.write(new Text(newVal), new Text(Float.toString(similarity)));
							 similarDocs++;
						 }
						 comparisons.add(minId+","+maxId);
						 numberComparisons++;
					 }
				 }
				 
			 }
	      }

	}
	 
	 

}
