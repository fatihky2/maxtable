import java.io.IOException;
import java.lang.Character;
import java.text.ParseException;
import java.util.Date;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.maxtable.mapreduce.*;
import org.maxtable.client.MtAccess;

public class mapreduce {
	  
	  public static class TokenizerMapper
	       extends org.maxtable.mapreduce.Mapper<BytesWritable, BytesWritable>{

	    
	    public void map(BytesWritable key, BytesWritable value, Context context
	                    ) throws IOException, InterruptedException {
	      
	      byte [] value_bytes = value.getBytes();
	      byte [] key_bytes = key.getBytes();
	      String key_str = new String(key_bytes);
	      String value_str = new String(value_bytes);
	      
	      System.out.println(key_str);
	      
	      byte [] value1 = MtAccess.mtGetValue(value_bytes, 1);
	      String value1_str = new String(value1);
	      
	      
	      System.out.println(value1_str);
	      String sub = "5000";
	      if(value1_str.indexOf(sub, 0) >= 0)
	      {
	    	  //context.write(key, value);
	    	  context.write(key, new BytesWritable(value1));

	          context.progress();
	      }
	    }
	  }

	  
	  public static class IntSumReducer
	       extends Reducer<BytesWritable,BytesWritable,BytesWritable,BytesWritable> {
	    

	    public void reduce(BytesWritable key, Iterable<BytesWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      BytesWritable value = null;
	      for (BytesWritable val : values) {
	        value = val;
	      }
	      
	      context.write(key, value);
	      context.progress();
	    }
	  }

	  

	  
	  public static void main(String[] args) throws Exception {
        
	    Configuration conf = new Configuration();
	    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    Job job = new Job(conf, "wikipedia");
	    job.setJarByClass(mapreduce.class);
	    //job.setCombinerClass(IntSumReducer.class);

	    
	    Utils.initMapperJob("test", "127.0.0.1", 1959,
	                         TokenizerMapper.class, BytesWritable.class, BytesWritable.class, job);

	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(BytesWritable.class);
	    job.setOutputValueClass(BytesWritable.class);
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://lubaosheng:9000/user/lubaosheng/maxtable"));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
