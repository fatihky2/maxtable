

package org.maxtable.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class Utils {

  public static void initMapperJob(String table, String ip, int port,
      Class<? extends Mapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<? extends Writable> outputValueClass, Job job) throws IOException {
    job.setInputFormatClass(InputFormat.class);
    if (outputValueClass != null)
      job.setMapOutputValueClass(outputValueClass);
    if (outputKeyClass != null)
      job.setMapOutputKeyClass(outputKeyClass);
    job.setMapperClass(mapper);
    
    job.getConfiguration().set(InputFormat.MT_TABLE, table);
    job.getConfiguration().set(InputFormat.MT_IP, ip);
    job.getConfiguration().set(InputFormat.MT_PORT, String.valueOf(port));
  }



}
