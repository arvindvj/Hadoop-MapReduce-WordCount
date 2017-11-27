import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class IIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
	  
        private final static Text w = new Text();
        private final static Text doc = new Text();

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            doc.set(fileName);
            String line = val.toString();
            String words[]=line.split(" ");
            for (String token : words){
                w.set(token);
                context.write(w,doc);
            }
        }
  }

  public static class IIndexReducer extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
        HashMap<String,Integer> indexmap=new HashMap<String,Integer>();

        for (Text txt:values){
            String ind=txt.toString();
            if (indexmap!=null && indexmap.containsKey(ind))
            {
               indexmap.put(ind,indexmap.get(ind)+1); 
            }
            else{
                indexmap.put(ind,1);
            }
        }
        context.write(key, new Text(indexmap.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "WordCount");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(IIndexMapper.class);
    job.setReducerClass(IIndexReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}