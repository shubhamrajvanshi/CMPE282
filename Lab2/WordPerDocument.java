package hadoop282;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordPerDocument extends Configured implements Tool {
    private static final String OUTPUT_PATH = "WordCountDocs";

    private static final String INPUT_PATH = "Freqwords";

    public static class WordCountsForDocsMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text document = new Text();
        private Text words = new Text();

        public WordCountsForDocsMapper() {
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] combinecount = value.toString().split("\t");
            String[] wordAndDoc = combinecount[0].split("@");
            document.set(wordAndDoc[1]);
            words.set(wordAndDoc[0] + "=" + combinecount[1]);
            context.write(document, words);
            
            
        }
    }

    public static class WordCountsForDocsReducer extends Reducer<Text, Text, Text, Text> {

        private Text wordAtDoc = new Text();
        private Text wordAvar = new Text();

        public WordCountsForDocsReducer() {
        }
        
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumOfWordsInDocument = 0;
            Map<String, Integer> tempCounter = new HashMap<String, Integer>();
            for (Text val : values) {
                String[] wordCounter = val.toString().split("=");
                tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
                sumOfWordsInDocument += Integer.parseInt(val.toString().split("=")[1]);
            }
            for (String wordKey : tempCounter.keySet()) {
                this.wordAtDoc.set(wordKey + "@" + key.toString());
                this.wordAvar.set(tempCounter.get(wordKey) + "/" + sumOfWordsInDocument);
                context.write(wordAtDoc,wordAvar);
            }
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "Words Counts");

        job.setJarByClass(WordPerDocument.class);
        job.setMapperClass(WordCountsForDocsMapper.class);
        job.setReducerClass(WordCountsForDocsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordPerDocument(), args);
        System.exit(res);
    }
}