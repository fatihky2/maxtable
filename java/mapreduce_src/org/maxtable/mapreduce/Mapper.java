
package org.maxtable.mapreduce;

import org.apache.hadoop.io.BytesWritable;

/**
 * Extends the base <code>Mapper</code> class to add the required input key 
 * and value classes.
 * 
 * @param <KEYOUT>  The type of the key.
 * @param <VALUEOUT>  The type of the value.
 * @see org.apache.hadoop.mapreduce.Mapper
 */
public abstract class Mapper<KEYOUT, VALUEOUT>
extends org.apache.hadoop.mapreduce.Mapper<BytesWritable, BytesWritable, KEYOUT, VALUEOUT> {

}
