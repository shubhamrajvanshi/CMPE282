package hadoopdemo;


import java.io.IOException;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Word_count {
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
 private final static IntWritable one = new IntWritable(1);
	     private Text word = new Text();
	
	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       //PorterStemmer ps= new PorterStemmer();
	      // System.out.println("CP1");
	    	 String line = (value.toString()).toLowerCase();
	    	
	    	String fixedInput = line.replaceAll("([\\.,'_\"*()$#/-])", "");
			//System.out.println(fixedInput);
			//System.out.println("CP2");
			//PorterStemmer.main(fixedInput);
		       
	       StringTokenizer tokenizer = new StringTokenizer(fixedInput);
	       while (tokenizer.hasMoreTokens()) {
	         word.set(tokenizer.nextToken());
	         output.collect(word, one);
	       }
	     }
	   }
	
	   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       int sum = 0;
	       //System.out.println("KEY IS "+key);
	       //System.out.println("VALUES: "+values.next());
	       while (values.hasNext()) {
	    	   sum += values.next().get();
	       }
	       
	       output.collect(key, new IntWritable(sum));
	     }
	   }
	
public static void main(String[] args) throws Exception {
	    
		   Configuration config = new Configuration();
		   
			config.set("fs.default.name","hdfs://localhost:9000");
			config.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			config.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

			FileSystem dfs = FileSystem.get(config);  
		   
		 JobConf conf = new JobConf(MapReduceBase.class);
	     conf.setJobName("wordcount");
	    
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(IntWritable.class);
	
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path("input"));
	     FileOutputFormat.setOutputPath(conf, new Path("output1"));
	
	     JobClient.runJob(conf);
	   }

	}
	