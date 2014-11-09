package hadoop282;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordsInCorpusTFIDF extends Configured implements Tool {
    
    // where to put the data in hdfs when we're done
    private static final String OUTPUT = "Freqwords";
    
    // where to put the data in hdfs when we're done
    private static final String OUTPUT_2 = "Worddocs";

    public static class WordsInCorpusTFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

        public WordsInCorpusTFIDFMapper() {
        }

        private Text wordAndDoc = new Text();
        private Text wordAndCounters = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] wordAndCounters = value.toString().split("\t");
            String[] wordAndDoc = wordAndCounters[0].split("@");
            this.wordAndDoc.set(new Text(wordAndDoc[0]));
            this.wordAndCounters.set(wordAndDoc[1] + "=" + wordAndCounters[1]);
            context.write(this.wordAndDoc, this.wordAndCounters);
        }
    }

    public static class WordsInCorpusTFIDFReducer extends Reducer<Text, Text, Text, Text> {

        private static final DecimalFormat DF = new DecimalFormat("###.########");

        private Text wordAtDocument = new Text();

        private Text tfidfCounts = new Text();

        public WordsInCorpusTFIDFReducer() {
        }
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            int numberOfDocumentsInCorpus = context.getConfiguration().getInt("numberOfDocsInCorpus", 0);
            int numberOfDocumentsInCorpusWhereKeyAppears = 0;
            Map<String, String> tempFrequencies = new HashMap<String, String>();
            for (Text val : values) {
                String[] documentAndFrequencies = val.toString().split("=");
                if (Integer.parseInt(documentAndFrequencies[1].split("/")[0]) > 0) {
                    numberOfDocumentsInCorpusWhereKeyAppears++;
                }
                tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
            }
            for (String document : tempFrequencies.keySet()) {
                String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
                double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0]) / Double.valueOf(wordFrequenceAndTotalWords[1]));
                double idf = Math.log10((double) numberOfDocumentsInCorpus / 
                   (double) ((numberOfDocumentsInCorpusWhereKeyAppears == 0 ? 1 : 0) + 
                         numberOfDocumentsInCorpusWhereKeyAppears));

                double tfIdf = tf * idf;

                wordAtDocument.set(key + "@" + document);
                tfidfCounts.set("[" + numberOfDocumentsInCorpusWhereKeyAppears + "/"
                        + numberOfDocumentsInCorpus + " , " + wordFrequenceAndTotalWords[0] + "/"
                        + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]");

                context.write(this.wordAtDocument, this.tfidfCounts);
            }
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        if (args[0] == null || args[1] == null) {
            System.out.println("You need to provide the arguments of the input and output");
            System.out.println(WordsInCorpusTFIDF.class.getSimpleName() + " prot:///path/to/input prot:///path/output");
            System.out.println(WordsInCorpusTFIDF.class.getSimpleName() + " -conf  /path/to/input /path/to/output");
        }

        Path userInputPath = new Path(args[0]);

        // Remove the user's output path
        Path userOutputPath = new Path(args[3]);
        if (fs.exists(userOutputPath)) {
            fs.delete(userOutputPath, true);
        }

        // Remove the phrase of word frequency path
      Path wordFreqPath = new Path(OUTPUT);
        if (fs.exists(wordFreqPath)) {
            fs.delete(wordFreqPath, false);
        }

        // Remove the phase of word counts path
        Path wordCountsPath = new Path(OUTPUT_2);
        if (fs.exists(wordCountsPath)) {
            fs.delete(wordCountsPath, false);
        }

        //Getting the number of documents from the user's input directory.
        FileStatus[] userFilesStatusList = fs.listStatus(userInputPath);
        final int numberOfUserInputFiles = userFilesStatusList.length;
        String[] fileNames = new String[numberOfUserInputFiles];
        for (int i = 0; i < numberOfUserInputFiles; i++) {
            fileNames[i] = userFilesStatusList[i].getPath().getName(); 
        }

        Job job = new Job(conf, "Word Frequency");
        job.setJarByClass(WordFrequency.class);
        job.setMapperClass(WordFrequency.WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequency.WordFrequenceInDocReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, userInputPath);
        TextOutputFormat.setOutputPath(job, new Path(OUTPUT));

        job.waitForCompletion(true);

        Configuration conf2 = getConf();
        conf2.setStrings("documentsInCorpusList", fileNames);
        Job job2 = new Job(conf2, "Words Counts");
        job2.setJarByClass(WordPerDocument.class);
        job2.setMapperClass(WordPerDocument.WordCountsForDocsMapper.class);
        job2.setReducerClass(WordPerDocument.WordCountsForDocsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job2, new Path(OUTPUT));
        TextOutputFormat.setOutputPath(job2, new Path(OUTPUT_2));

        job2.waitForCompletion(true);

        Configuration conf3 = getConf();
        conf3.setInt("numberOfDocsInCorpus", 994);
        Job job3 = new Job(conf3, "TF-IDF of Words in Corpus");
        job3.setJarByClass(WordsInCorpusTFIDF.class);
        job3.setMapperClass(WordsInCorpusTFIDFMapper.class);
        job3.setReducerClass(WordsInCorpusTFIDFReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job3, new Path(OUTPUT_2));
        TextOutputFormat.setOutputPath(job3, userOutputPath);
        
        
        return job3.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordsInCorpusTFIDF(), args);
        System.exit(res);
    }
}