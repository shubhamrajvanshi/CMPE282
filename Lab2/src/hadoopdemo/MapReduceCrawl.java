package hadoopdemo;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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


public class MapReduceCrawl {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text word = new Text();
		private Text linkCSV = new Text();
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException
		{
			LinkGetter linkGetter = new LinkGetter("");
			String line = value.toString();
			List<String> anchors = linkGetter.getLinks(line);
			StringBuilder links = new StringBuilder();
			for(String s:anchors)
			{
				links.append(s);
				links.append(",");
			}
			
			word.set(line);
			linkCSV.set(links.toString());
			try {
				context.write(word, linkCSV);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException
		{
			Iterator<Text> iterator = values.iterator();
			String links = "";
			// This is a bad code but just an example
			while(iterator.hasNext())
			{
				links = iterator.next().toString();				
			}
			try {
				context.write(key, new Text(links));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		System.out.println("Started Job");
		Job job = new Job(conf, "crawler");
		job.setJarByClass(MapReduceCrawl.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}

}
