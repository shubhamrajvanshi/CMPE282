/**
 * 
 */
package problem2;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * @author shubham
 *
 */
public class Driver2 {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Path output = new Path("problem2");
		File temp = new File(output.toString());
		if(temp.isDirectory()&& temp.exists())
		{
			FileUtils.deleteDirectory(temp);
			System.out.println("Deleting Folder");
		}
		Date start = new Date();
		   System.out.println("Start........................... Program starting time: "+start);
		Configuration conf = new Configuration();
		JobConf job = new JobConf(Driver2.class);
		
		job.setJobName("Driver2");
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        job.setMapperClass(mymapper.class);
        job.setReducerClass(myreducer.class);
        	
        job.setInputFormat(TextInputFormat.class) ;
        job.setOutputFormat(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        JobClient.runJob(job);
        Date end=new Date();
	     
	    System.out.println("End............................. Program end time: "+end);
	    Long diff=(end.getTime()-start.getTime());
	    System.out.println("The program Driver2's start time is: "+start+" , and end time is: "+end);
	    System.out.println("Total time of execution: "+diff+" milliseconds.");

	}

}
