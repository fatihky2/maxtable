package org.maxtable.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.maxtable.client.MtAccess;


public class InputFormat
extends org.apache.hadoop.mapreduce.InputFormat<BytesWritable, BytesWritable> {

  final Log LOG = LogFactory.getLog(InputFormat.class);

  public static final String MT_TABLE = "maxtable.mapreduce.input.table";
  public static final String MT_IP = "maxtable.mapreduce.input.mt_ip";
  public static final String MT_PORT = "maxtable.mapreduce.input.mt_port";

  private MtAccess m_client = null;
  
  private String m_tablename = null;
 
  private String m_maxtable_ip = null;
  private int m_maxtable_port = 0;

  protected class RecordReader
  extends org.apache.hadoop.mapreduce.RecordReader<BytesWritable, BytesWritable> {

    private MtAccess m_client = null;
    
    private String m_tablename = null;
    
    private long m_readerptr = 0;
    //private TableSplit m_split = null;
    
    //private boolean m_eos = false;
    private long m_bytes_read = 0;

    private BytesWritable m_key = null;//new Text();
    private BytesWritable m_value = null;

    /**
     *  Constructor
     *
     */
    public RecordReader(MtAccess client, String tablename) {
      m_client = client;
      m_tablename = tablename;
      //m_split = ts;
    }

    /**
     * Initializes the reader.
     */
    @Override
    public void initialize(InputSplit inputsplit,
        TaskAttemptContext context) throws IOException,
        InterruptedException {
      //try {
        TableSplit ts = (TableSplit)inputsplit;
        MtAccess.Split split = new MtAccess.Split();
        
        split.tableName = ts.m_tablename;
        split.tabletName = ts.m_tabletname;
        split.rangeIp = ts.m_rangeip;
        split.metaIp = ts.m_metaip;
        split.rangePort = Integer.parseInt(ts.m_rangeport);
        split.metaPort = Integer.parseInt(ts.m_metaport);
        
        m_readerptr = m_client.mtCreateReader(split);
      //}
    }

    /**
     * Closes the split.
     *
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public void close() {
      try {
        m_client.mtFreeReader(m_readerptr);
        m_client.mtCloseConnection();
        MtAccess.mtDestroyContext();
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return A number between 0.0 and 1.0, the fraction of the data read.
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override
    public float getProgress() {
      // Assume 200M split size
      if (m_bytes_read >= 200000000)
        return (float)1.0;
      return (float)m_bytes_read / (float)200000000.0;
    }

    /**
     * Returns the current key.
     *
     * @return The current key.
     * @throws IOException
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public BytesWritable getCurrentKey() throws IOException,
        InterruptedException {
      return m_key;
    }

    /**
     * Returns the current value.
     *
     * @return The current value.
     * @throws IOException When the value is faulty.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
      return m_value;
    }

    /**
     * Positions the record reader to the next record.
     *
     * @return <code>true</code> if there was another record.
     * @throws IOException When reading the record failed.
     * @throws InterruptedException When the job was aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      try {
        MtAccess.cRet ret = m_client.mtGetNextKeyValue(m_readerptr);
        long value_ptr = ret.ptr;
        int value_len = ret.length;
        //System.out.println("current value length: " + value_ptr);
        /*if (value_ptr == 0)
        {
        	System.out.println("just for test");
        }*/
        
        if (value_ptr == 0)
        {
          System.out.println("There is no next value for this map task!");
          return false;
        }
        
        byte [] value_key = m_client.mtGetKey(m_readerptr, value_ptr);
        byte [] value_value = m_client.mtGetValues(m_readerptr, value_ptr, value_len);
        
        m_key = new BytesWritable(value_key);
        m_value = new BytesWritable(value_value);
        
        m_bytes_read += value_key.length + value_value.length;
        
        //System.out.println("get next value!" + value_value.length);
        
        return true;
      }
      finally
      {
        //return true;
      }
    }

  }

  /**
   * Builds a RecordReader.
   *
   * @param split  The split to work with.
   * @param context  The current context.
   * @return The newly created record reader.
   * @throws IOException When creating the reader fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(
   *   org.apache.hadoop.mapreduce.InputSplit,
   *   org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public org.apache.hadoop.mapreduce.RecordReader<BytesWritable, BytesWritable>
    createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    try {
      //System.out.println("java create reader!");
      TableSplit ts = (TableSplit)split;
      /*System.out.println(ts.m_tablename);
      System.out.println(ts.m_tabletname);
      System.out.println(ts.m_rangeip);
      System.out.println(ts.m_rangeport);
      System.out.println(ts.m_metaip);
      System.out.println(ts.m_metaport);*/

      if (m_tablename == null) {
        m_tablename = context.getConfiguration().get(MT_TABLE);
        
        System.out.println(m_tablename);
      }

      if (m_client == null){
        m_client_init(context, false);
      }
      return new RecordReader(m_client, m_tablename);
    }
    
    finally {
      //m_client.mtCloseConnection();
        
      //MtAccess.mtDestroyContext();
    }

  }
  
  public void m_client_init(JobContext context, boolean conn) throws IOException{
	m_maxtable_ip = context.getConfiguration().get(MT_IP);
    String port = context.getConfiguration().get(MT_PORT);
    m_maxtable_port = Integer.parseInt(port);
    MtAccess.mtCreateContext();
    m_client = new MtAccess(m_maxtable_ip, m_maxtable_port, conn);
    //m_client = ThriftClient.create("localhost", 38080);	
  }

  /**
   * Calculates the splits that will serve as input for the map tasks. The
   * number of splits matches the number of ranges in a table.
   *
   * @param context  The current job context.
   * @return The list of input splits.
   * @throws IOException When creating the list of splits fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
   *   org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {

    long ns=0;
    try {
      //RowInterval ri = null;

      if (m_client == null){
        m_client_init(context, true);
      }

      //String namespace = context.getConfiguration().get(NAMESPACE);
      String tablename = context.getConfiguration().get(MT_TABLE);

      //ns = m_client.namespace_open(namespace);
      List<MtAccess.Split> mtsplits = m_client.mtGetSplits(tablename);
      
      List<InputSplit> splits = new ArrayList<InputSplit>(mtsplits.size());
      
      for (final MtAccess.Split ts : mtsplits) {
        TableSplit split = new TableSplit(ts);
        splits.add(split);
      }
    
      return splits;
    }

    catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    finally {
      m_client.mtCloseConnection();
      
      MtAccess.mtDestroyContext();
    }

  }

}
