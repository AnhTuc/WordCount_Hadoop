
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.StringUtils;

import org.apache.log4j.Logger;

public class WordCountv02 extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(WordCountv02.class);
  
  public static void main(String[] args) throws Exception {
	//Invokes ToolRUnner to run new Instace of WordCount first starting
    int res = ToolRunner.run(new WordCountv02(), args);
    
    //Pass status to System object
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
	 //Create instance of Object Job with name "wordcount" and configuration getConf()
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    //Add input and output directory
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    
    //Set output format
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private boolean caseSensitive = false;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    protected void setup(Mapper.Context context)
      throws IOException,
        InterruptedException {
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      
      if (!caseSensitive) {
        line = line.toLowerCase();
      }
      
      Text temp = new Text();
      
        for (String word : WORD_BOUNDARY.split(line)) {
        	//In case there is no word or non-word characters
          if (word.isEmpty() || Pattern.matches("\\W*", word)) continue;
      
          temp = new Text(word);
          context.write(temp,one);
        }         
      }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}