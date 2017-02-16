package assignement1;

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



public class InvertedIndex_ii extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new InvertedIndex_ii(), args);
	      
	      System.exit(res);
	   }
	
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "InvertedIndex_ii");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); //We use ";" as a delimitor in the output file instead of tab
	      job.setJarByClass(InvertedIndex_ii.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("input")); 
	      Path outputPath = new Path("output/InvertedIndex_ii");
	      FileOutputFormat.setOutputPath(job, outputPath);
	      FileSystem hdfs = FileSystem.get(getConf());
		  if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		  }
		  
		  //Uncomment the two next lines if you want to add the input and output folder in the param of the job
	      //FileInputFormat.addInputPath(job, new Path(args[0]));
	      //FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      // Print the counter in the output
	      //System.out.println("CustomCounter.UniqueWords = " + job.getCounters().findCounter(CustomCounter.UniqueWords).getValue());
	      
	      
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
		  HashMap <String, Integer> counter = new HashMap<String, Integer>();
	      
	      protected void setup(Context context) throws IOException, InterruptedException { 
	          counter.put("uniqueWords", 0);
	          counter.put("wordInSingleDocument", 0);
	       }
		 
		 @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	         //Initialization of the variables
	    	 HashSet<String> hashset = new HashSet<String>();
	    	 String file = new String();
	    	 
	    	 //We put every diffrent filename into the hashset
	    	 for (Text value : values){
	    		 
	    		 file = value.toString();
	    		
	    		 if(!hashset.contains(file)){
	    			 hashset.add(file);
	    		 }
	    	 }
	    	 
	    	 //If the word appear only once in the a given file, we increment the counter wordInSingleDocument
	    	 if(hashset.size()==1){
	    		 counter.put("wordInSingleDocument",counter.get("wordInSingleDocument")+1);
	    	 }
	    	 //And we increment the counter UniqueWord
	    	 counter.put("uniqueWords",counter.get("uniqueWords")+1);
	    	 context.write(key, new Text());
	      }
		 
		 protected void cleanup(Context context) throws IOException, InterruptedException {
		       try{
		          Path pt=new Path("output/InvertedIndex_ii/Counter.txt");
		          FileSystem fs = FileSystem.get(new Configuration());
		          BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
		          String line1="Number of unique words : " + counter.get("uniqueWords") + "\n";
		          String line2="Number of words in single document : " + counter.get("wordInSingleDocument") + "\n";
		          br.write(line1);
		          br.write(line2);
		          br.close();
		    }catch(Exception e){
		            System.out.println("Exception caught");
		    }
		}
	}
	 
	 

}
