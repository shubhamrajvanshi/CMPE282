package problem3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordFrequency extends Configured implements Tool {

    private static final String OUTPUT = "wordfrequency";

    public static class WordFrequenceInDocMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static Set<String> removewords;
        public int filecount=0;
        public int count=0;

        static {
            removewords = new HashSet<String>();
            removewords.add("I"); removewords.add("a"); removewords.add("about");removewords.add("an"); removewords.add("are"); removewords.add("as");removewords.add("at"); removewords.add("be"); removewords.add("by");
            removewords.add("com"); removewords.add("de"); removewords.add("en"); removewords.add("for"); removewords.add("from"); removewords.add("how");
            removewords.add("in"); removewords.add("is"); removewords.add("it"); removewords.add("la"); removewords.add("of"); removewords.add("on");
            removewords.add("or"); removewords.add("that"); removewords.add("the");removewords.add("this"); removewords.add("to"); removewords.add("was");
            removewords.add("what"); removewords.add("when"); removewords.add("where"); removewords.add("who"); removewords.add("will"); removewords.add("with");
            removewords.add("and"); removewords.add("the"); removewords.add("www");
        }

        private static final Pattern P1 = Pattern.compile("\\w+");

        private Text word = new Text();
        private IntWritable singleCount = new IntWritable(1);

        public WordFrequenceInDocMapper() {
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          count++;
          if(count%35==0)
          filecount++;
          
        	Matcher m = P1.matcher(value.toString());
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            StringBuilder valueBuilder = new StringBuilder();
            while (m.find()) {
                String matchedKey = m.group().toLowerCase();
                if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
                        || removewords.contains(matchedKey) || matchedKey.contains("_") || 
                            matchedKey.length() < 3) {
                    continue;
                }
                valueBuilder.append(matchedKey);
               valueBuilder.append("@");
               valueBuilder.append(filecount);
                word.set(valueBuilder.toString());
                context.write(this.word, singleCount);
                valueBuilder.setLength(0);
                
            }
            System.out.println(filecount);
        }
    }

    public static class WordFrequenceInDocReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable wordSum = new IntWritable();
        
        public WordFrequenceInDocReducer() {
        }
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
                 InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            wordSum.set(sum);
            context.write(key, this.wordSum);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "Word Frequence In Document");

        job.setJarByClass(WordFrequency.class);
        job.setMapperClass(WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequenceInDocReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordFrequency(), args);
        System.exit(res);
    }
}