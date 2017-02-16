package assignement1;

import java.io.IOException;
import java.util.Arrays;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StopWords_i extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new StopWords_i(), args);
	      
	      System.exit(res);
	   }
	
	
	@Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "StopWords_i");
	      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); //We use ; as a delimitor in the output file instead of tab
	      job.setJarByClass(StopWords_i.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);
	      
	      job.setNumReduceTasks(10); //Set the number of reducers to 10

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      
	      FileInputFormat.addInputPath(job, new Path("input")); 
	      Path outputPath = new Path("output/StopWords_i");
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

	 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	      private final static IntWritable ONE = new IntWritable(1);
	      private Text word = new Text();

	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	         for (String token: value.toString().replaceAll("[^0-9A-Za-z]"," ").split("\\s+")) {
	        	token = token.toLowerCase();
	            word.set(token);
	            context.write(word, ONE);
	         }
	      }
	   }
	 
	 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	      @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
	         int sum = 0;
	         for (IntWritable val : values) {
	            sum += val.get();
	         }
	         if(sum>=4000){
	        	 context.write(key, new IntWritable(sum));
	         }
	      }
	}
	 
	 

}
