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
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class TopkCommonWords {      
    public static class TokenMapper
        extends Mapper<Object, Text, Text, IntWritable>{        
        
        // private KV kv = new KV();
        private IntWritable docID = new IntWritable();
        private Text word = new Text();
        private HashSet<String> stopWords;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String path = conf.get("stop words");
            File file = new File(path);
            BufferedReader br = new BufferedReader(new FileReader(file));

            stopWords = new HashSet<String>();
            String st;
            while ((st = br.readLine()) != null) {
                stopWords.add(st);
            }
        }

        @Override
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
                String w = itr.nextToken();
                if (stopWords.contains(w)) {
                    continue;
                }

                word.set(w);
                docID.set(dID);
                context.write(word, docID);
            }
        }
    }

    public static class TokenReducer 
        extends Reducer<Text, IntWritable, IntWritable, Text> {
        
        private Integer result;
        // private Text word = new Text();
        private Map<Integer, Text> map = new TreeMap<Integer, Text>(Collections.reverseOrder());
        private LinkedHashMap<Integer, Text> reverseSortedMap = new LinkedHashMap<>();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> docIDs, Context context) 
            throws IOException, InterruptedException {
            
            Integer sum_doc1 = 0;
            Integer sum_doc2 = 0;

            for (IntWritable dID : docIDs) {
                if (dID.get() == 1) {
                    sum_doc1 += 1;
                } else {
                    sum_doc2 += 1;
                }
            }

            if (sum_doc1 < sum_doc2) {
                result = sum_doc2;
            } else {
                result = sum_doc2;
            }
            Text word = new Text(key.toString());

            map.put(result, word);
            // IntWritable a = new IntWritable(result);  
            // context.write(a, key);
        }

        @Override
        protected void cleanup(Context context) 
            throws IOException, InterruptedException{
            int i = 0;
            for (Map.Entry<Integer, Text> entry : map.entrySet()) {
                if (i == 20) {
                    break;
                }
                
                IntWritable occurence = new IntWritable(entry.getKey());
                context.write(occurence, entry.getValue());
                i += 1;
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Path stopWords = new Path(args[2]);
        
        Configuration conf = new Configuration();
        conf.set("stop words", stopWords.toString());

        Job job = Job.getInstance(conf, "top word");
        job.setJarByClass(TopkCommonWords.class);
        job.setMapperClass(TokenMapper.class);
        job.setReducerClass(TokenReducer.class);  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
