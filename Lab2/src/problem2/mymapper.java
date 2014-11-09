package problem2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;	
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author shubham
 *
 */
public class mymapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text file ;
		
		public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
			
			String line = value.toString().toLowerCase().replaceAll("([!?.;,'_\"*()$#/-:@])", "");
		
			if (line.contains("buck") && line.contains("mulligan"))	
			{		
			//	System.out.println(line);
				word.set("Buck and Mulligan together: ");
				output.collect(word, one);
				}
			else if(line.contains("sherlock") && line.contains("holmes")){
			//	System.out.println(line);
				word.set("Sherlock and holmes together: ");
				output.collect(word, one);
			}
			else if((line.contains("buck") && line.contains("mulligan")) && (line.contains("sherlock") && line.contains("holmes")) ){
			//	System.out.println(line);
				word.set("Sherlock and holmes together: ");
				output.collect(word, one);
			}
		}
	}


