// Matric Number: A0219731E 
// Name: Nguyen Minh tuan
// TopkCommonWords.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.*;
import java.util.StringTokenizer;

public class TopkCommonWords {

    // public static class KV {
        
    //     private Text key;
    //     private Text value;

    //     public KV() {
    //         key = new Text("");
    //         value = new Text("");
    //     }

    //     public Text getVal() {
    //         return value;
    //     }

    //     public Text getKey() {
    //         return key;
    //     }

    //     public void setKey(String t) {
    //         key.set(t);
    //     }

    //     public void setVal(String t) {
    //         value.set(t);
    //     }
    // }
        
    public static class TokenMapper
        extends Mapper<Object, Text, Text, IntWritable>{        
        
        // private KV kv = new KV();
        private IntWritable docID = new IntWritable();
        private Text word = new Text();
        private Set<String> stopWords;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            stopWords = new HashSet<String>();
            for(String word : conf.get("stop words").split(",")) {
                stopWords.add(word);
            }
        }

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            int dID;

            if ("task1-input1.txt".equals(fileName)) {
                dID = 1;
            } else {
                dID = 2;
            }

            while (itr.hasMoreTokens()) {
                // kv.setKey(fileName);
                // kv.setVal(itr.nextToken());
                word.set(itr.nextToken());
                docID.set(dID);
                context.write(word, docID);
            }
        }
    }

    public static class TokenReducer 
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> docIDs, Context context) 
            throws IOException, InterruptedException {
            
            int sum_doc1 = 0;
            int sum_doc2 = 0;

            for (IntWritable dID : docIDs) {
                if (dID.get() == 1) {
                    sum_doc1 += 1;
                } else {
                    sum_doc2 += 1;
                }
            }

            if (sum_doc1 < sum_doc2) {
                result.set(sum_doc1);
            } else {
                result.set(sum_doc2);
            }

            context.write(key, result);
        }

    }

    // public static class OccurenceReducer
    //     extends Reducer<Text, IntWritable, Text, IntWritable> {
    //     private IntWritable result = new IntWritable();

    //     public void reduce(Text key, Iterable<IntWritable> values,
    //                     Context context
    //                     ) throws IOException, InterruptedException {
    //         int min = 0;
    //         for (IntWritable val : values) {
    //             // min = (min > val.get()) ? val.get() : min;
    //             min = val.get();
    //         }

    //         result.set(min);
    //         context.write(key, result);
    //     }
    // }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("stop words", new Path(args[2]));
        Job job = Job.getInstance(conf, "top word");
        job.setJarByClass(TopkCommonWords.class);
        job.setMapperClass(TokenMapper.class);
        // job.setCombinerClass(TokenReducer.class);
        job.setReducerClass(TokenReducer.class);  
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
