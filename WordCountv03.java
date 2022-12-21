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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class WordCountv03 extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(WordCountv03.class);
  
  public static void main(String[] args) throws Exception {
	//Invokes ToolRUnner to run new Instace of WordCount first starting
    int res = ToolRunner.run(new WordCountv03(), args);
    
    //Pass status to System object
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
	 //Create instance of Object Job with name "wordcount" and configuration getConf()
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    
    //======01====Check and find stop_word.txt
    int i=0;
    while(i<args.length) {
    	if("-skip".equals(args[i])) {
    		job.addCacheFile(new Path(args[i+1]).toUri());
    		LOG.info("Add file to Cache: "+args[i+1]);
    		break;
    	}
    	i+=1;
    }
    

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
    
    private String input;
    private Set<String> patternsToSkip = new HashSet<String>();

    protected void setup(Mapper.Context context)
      throws IOException,
        InterruptedException {
      if (context.getInputSplit() instanceof FileSplit) {
    		  this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
    		} else {
    		  this.input = context.getInputSplit().toString();
    		}
      
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
      URI[] localPaths=context.getCacheFiles();
      parseFile(localPaths[0]);
    }
    
    private void parseFile(URI patternsURI) {
    	try {
            BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
            String pattern;
            while ((pattern = fis.readLine()) != null) {
              patternsToSkip.add(pattern);
            }
          } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '"
                + patternsURI + "' : " + StringUtils.stringifyException(ioe));
          }
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
          if(patternsToSkip.contains(word)) continue;
      
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