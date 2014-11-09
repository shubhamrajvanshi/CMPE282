package problem1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class mymapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text file ;
		
		public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			
			String line = value.toString().toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit(); 
			String fileName = fileSplit.getPath().getName();
			
			file = new Text(fileName+"@"+key.toString());
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().toString());
				output.collect(word, file);
			}
		}
	}


