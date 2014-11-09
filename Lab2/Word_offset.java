package offset;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.*;

public class Word_offset {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		     private Text word = new Text();
		     private Text file_offset = new Text();
		     private int filePos = 0;
		     
		     public void map (LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		     
		    	String line = (value.toString()).toLowerCase();
		    	 FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			    	String fileName = fileSplit.getPath().getName();
			    	byte[] buf = line.getBytes();
			    	StringTokenizer tokenizer = new StringTokenizer(line);
				    while (tokenizer.hasMoreTokens()) {
				    	String temp=tokenizer.nextToken();
				    	StringBuilder valueBuilder = new StringBuilder();
			    		valueBuilder.append(fileName);
			            valueBuilder.append("@");
			            valueBuilder.append(filePos);
			            this.file_offset.set(valueBuilder.toString());
			            //Text file_offset= new Text((String) valueBuilder.toString());
			            this.word.set(temp.toString());
			            //Text word= new Text((String) temp.toString());
			            output.collect(this.word, this.file_offset);
				    
				    }
				    
				    filePos += buf.length; 
			  }
		  }
		
		   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		     
			   private Text word = new Text();
			   
			   
			   public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		       
		       
		       String combine = "";
		       //System.out.println(values);
		       while (values.hasNext()) {
		    	   combine= combine+" "+values.next();
		    	   //System.out.println(combine);
		       }
		       this.word.set(combine.toString());
		       output.collect(key, word);
		     }
		   }
		
		   public static void main(String[] args) throws Exception {
			   
			   Date start=new Date();
			   System.out.println("Start........................... Program start time: "+start);
			   Configuration config = new Configuration();

				config.set("fs.default.name","hdfs://localhost:9000");

				FileSystem dfs = FileSystem.get(config);  
			   
			 JobConf conf = new JobConf(WordCount.class);
		     conf.setJobName("problem1");
		    
		     conf.setOutputKeyClass(Text.class);
		     conf.setOutputValueClass(Text.class);
		
		     conf.setMapperClass(Map.class);
		     conf.setCombinerClass(Reduce.class);
		     conf.setReducerClass(Reduce.class);
		
		     conf.setInputFormat(TextInputFormat.class);
		     conf.setOutputFormat(TextOutputFormat.class);
		
		     FileInputFormat.setInputPaths(conf, new Path(args[0]));
		     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		     
		     JobClient.runJob(conf);
		     Date end=new Date();
		     
		     System.out.println("End............................. Program end time: "+end);
		     
		     Long diff=(end.getTime()-start.getTime());
		     
		     System.out.println("The program started at: "+start+" , and ended at: "+end);
		     System.out.println("Time Difference....................... Program took: "+diff+" milliseconds.");
		     
		   }
		}
