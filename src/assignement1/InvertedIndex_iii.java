package assignement1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InvertedIndex_iii extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new InvertedIndex_iii(), args);
	      
	      System.exit(res);
	   }
	
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "InvertedIndex_iii");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); //We use ";" as a delimitor in the output file instead of tab
	      job.setJarByClass(InvertedIndex_iii.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setMapperClass(Map.class);
	      job.setCombinerClass(Combine.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("input")); 
	      Path outputPath = new Path("output/InvertedIndex_iii");
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

	 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	      private Text word = new Text();
	      private Text fileName = new Text();


	     @Override
	     public void map(LongWritable key, Text value, Context context)
	             throws IOException, InterruptedException {
	    	 
	    	 //We read the file which contains the list of stopwords
	     	 File inputFile = new File("/home/cloudera/workspace/Antoine_Pupin/input/stopwords.txt");
	       	 BufferedReader read = new BufferedReader(new FileReader(inputFile));
	       	 
	       	 //First we read the file and we add each words into a list
	       	 Set<String> stopWords = new HashSet<String>();
	       	 String stopword = null;
	       	 while ((stopword = read.readLine()) != null){
	       		 stopWords.add(stopword);
	       	 }
	       	 read.close();
	        	 
	       	 
	         for (String token: value.toString().replaceAll("[^0-9A-Za-z]"," ").split("\\s+")){
	           	String nameOfFile = ((FileSplit) context.getInputSplit()).getPath().getName();
	           	fileName = new Text(nameOfFile);
	           	token = token.toLowerCase();
	           	if (!stopWords.contains(token)){
	           		word.set(token);
	           		context.write(word, fileName);
	           	}
	            }
	       }
	   }
	 
	 public static class Reduce extends Reducer<Text, Text, Text, Text> {
		 @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
			 //Initialization of the variables
	    	 HashSet<String> hashset = new HashSet<String>();
	    	 String file = new String();
	    	 String totalFile = new String();
	    	 
	    	 for (Text value : values){
	    		 file = value.toString();
	    		 if(!hashset.contains(file)){
	    			 hashset.add(file);
	    		 }
	    	 }
	    	 for (String value : hashset){
	    		 totalFile += value + ",";
	    	 }
	    	 totalFile = totalFile.substring(0, totalFile.length()-1);
	    	 context.write(key, new Text(totalFile));
	      }
	}
	 
	 
	 public static class Combine extends Reducer<Text, Text, Text, Text> {
		 @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	         //Initialization of the variables
	    	 HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
	    	 String file = new String();
	    	 String totalFile = new String();
	    	 Integer count = 0;
	    	 
	    	 //We put every different filename into the hashmap and we increment the counter in order to keep the number of occurence in each files
	    	 for (Text value : values){
	    		 
	    		 file = value.toString();
	    		
	    		 if(hashmap.get(file)!=null){
	    			 count = hashmap.get(file);
	    			 count++;
	    			 hashmap.put(file, ++count);
	    		 } 
	    		 else {	    			 
	    			 hashmap.put(file,1);
	    		 }
	    	 }
	    	 //We create a string containing every different filemane for a given key with the number of occurence in each files
	    	 for (String value : hashmap.keySet()){
	    		 totalFile += value + "#" + hashmap.get(value) + ",";
	    	 }
	    	 //We remove the last coma and we add the key, file names and number of occurence in the output
	    	 totalFile = totalFile.substring(0, totalFile.length()-1);
	    	 context.write(key, new Text(totalFile));
	      }
	}
	 
	 

}
