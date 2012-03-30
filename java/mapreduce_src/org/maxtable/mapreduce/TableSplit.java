
package org.maxtable.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.maxtable.mapreduce.Serialization;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import org.maxtable.client.MtAccess;
/**
 * A table split corresponds to a key range (low, high). All references to row
 * below refer to the key of the row.
 */
public class TableSplit extends InputSplit
implements Writable, Comparable<TableSplit> {

  String m_tablename;
  String m_tabletname;

  String m_rangeip;
  String m_rangeport;
  
  String m_metaip;
  String m_metaport;

  /** Default constructor. */
  /** Default constructor. */
  public TableSplit() {
      //this(new byte [0], new byte [0], new byte [0], "");
  }
  
  public TableSplit(MtAccess.Split split) {
      m_tablename = split.tableName;
      m_tabletname = split.tabletName;
      m_rangeip = split.rangeIp;
      m_metaip = split.metaIp;
      m_rangeport = String.valueOf(split.rangePort);
      m_metaport = String.valueOf(split.metaPort);
      
      //System.out.println(split.metaPort);
      //System.out.println(m_metaport);
  }
  
  /**
   * Returns the range location.
   *
   * @return The range's location.
   */
  public String getRangeLocation() {
    return m_rangeip;
  }

  /**
   * Returns the range's location as an array.
   *
   * @return The array containing the range location.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
   */
  @Override
  public String[] getLocations() {
    return new String[] {m_rangeip};
  }

  /**
   * Returns the length of the split.
   *
   * @return The length of the split.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
   */
  @Override
  public long getLength() {
    // Not clear how to obtain this... seems to be used only for sorting splits
    return 0;
  }


  /**
   * Reads the values of each field.
   *
   * @param in  The input to read from.
   * @throws IOException When reading the input fails.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
	m_tablename = Serialization.toString(Serialization.readByteArray(in));
	m_tabletname = Serialization.toString(Serialization.readByteArray(in));
    m_rangeip = Serialization.toString(Serialization.readByteArray(in));
    m_rangeport = Serialization.toString(Serialization.readByteArray(in));
    m_metaip = Serialization.toString(Serialization.readByteArray(in));
    m_metaport = Serialization.toString(Serialization.readByteArray(in));
  }

  /**
   * Writes the field values to the output.
   *
   * @param out  The output to write to.
   * @throws IOException When writing the values to the output fails.
   */
  @Override
  public void write(DataOutput out) throws IOException {
	Serialization.writeByteArray(out, Serialization.toBytes(m_tablename));
	Serialization.writeByteArray(out, Serialization.toBytes(m_tabletname));
	Serialization.writeByteArray(out, Serialization.toBytes(m_rangeip));
    Serialization.writeByteArray(out, Serialization.toBytes(m_rangeport));
    Serialization.writeByteArray(out, Serialization.toBytes(m_metaip));
    Serialization.writeByteArray(out, Serialization.toBytes(m_metaport));
  }

  /**
   * Returns the details about this instance as a string.
   *
   * @return The values of this instance as a string.
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return m_rangeip + ":" + m_tablename + ":" + m_tabletname;
  }
  
  /**
   * Compares this split against the given one.
   *
   * @param split  The split to compare to.
   * @return The result of the comparison.
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(TableSplit split) {
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof TableSplit)) {
      return false;
    }
    return Serialization.equals(Serialization.toBytes(m_tablename), Serialization.toBytes(((TableSplit)o).m_tablename)) &&
      Serialization.equals(Serialization.toBytes(m_tabletname), Serialization.toBytes(((TableSplit)o).m_tabletname)) &&
      Serialization.equals(Serialization.toBytes(m_rangeip), Serialization.toBytes(((TableSplit)o).m_rangeip));
  }
}
