
package org.maxtable.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.ArrayList;

public class MtAccess
{

	// the pointer in C
	private long connectionPtr;

	private final static native
	void createContext();
	
	private final static native
	void destroyContext();
	
	
	private final static native
	long openConnection(String metaServerHost, int metaServerPort);
	
	private final static native
	void closeConnection(long ptr);
	
	
	private final static native
	long openExecute(long ptr, String cmd);
	
	
	private final static native
	void closeExecute(long exePtr);
	
	
	private final static native
	long getNextrow(long exePtr);
	
	
	private final static native
	String getVarColValue(long exePtr, long rowBuf, int colIdx);

        private final static native
        int getFixedColValue(long exePtr, long rowBuf, int colIdx);

        private final static native
        int colTypeFixed(long exePtr, int colIdx);

	private final static native
	void getSplits(long ptr, String tableName, cRet ret);

	private final static native
	Split getSplit(long splitsPtr, int splitNum);

	private final static native
	void freeSplits(long splitsPtr);

	private final static native
	long createReader(Split split);

	private final static native
	void freeReader(long readerPtr);

	private final static native
	void getNextKeyValue(long readerPtr, cRet ret);

	private final static native
	void getKey(long readerPtr, long rowPtr, cRet ret);

	private final static native
	void getValue(long readerPtr, long rowPtr, int rowLength, cRet ret);

	private final static native
        void freeValue(long rowPtr);

	private final static native
        void arrayCopy(byte[] valueArray, long valuePtr, int valueLength);


    static {
        try {
            System.loadLibrary("mt_access");
        } catch (UnsatisfiedLinkError e) {
            e.printStackTrace();
            System.err.println("Unable to load mt_access native library: " + System.getProperty("java.library.path"));
            System.exit(1);
        }
    }

    public MtAccess(String metaServerHost, int metaServerPort, boolean conn) throws IOException
    {
	if(conn)
	{
        	connectionPtr = openConnection(metaServerHost, metaServerPort);
        	if (connectionPtr == 0) {
            		throw new IOException("Unable to initialize maxtable java client");
        	}
	}
    }

	public static void mtCreateContext()
	{
		createContext();
	}
	
	public static void mtDestroyContext()
	{
		destroyContext();
	}
	
	public void mtCloseConnection()
	{
		if(connectionPtr != 0)
		{
			closeConnection(connectionPtr);
		}
	}
	
	public long mtOpenExecute(String cmd)
	{
		return openExecute(connectionPtr, cmd);
	}
	
	public void mtCloseExecute(long exePtr)
	{
		closeExecute(exePtr);
	}
	
	public long mtGetNextrow(long exePtr)
	{
		return getNextrow(exePtr);
	}

	public static class ColValue 
	{
		public String value;
		public boolean typeFixed;
	}
	
	public ColValue mtGetColValue(long exePtr, long rowBuf, int colIdx)
	{
		ColValue colValue = new ColValue();

		int typeFixed = colTypeFixed(exePtr, colIdx);

		if(typeFixed == 0)
		{
			colValue.typeFixed = false;
			colValue.value = getVarColValue(exePtr, rowBuf, colIdx);
		}
		else
		{
			colValue.typeFixed = true;
			int value = getFixedColValue(exePtr, rowBuf, colIdx);
			colValue.value = String.valueOf(value);
		}
			
		return colValue;
	}

	public static class Split
	{
		public String	tableName;
		public String	tabletName;
		public String	rangeIp;
		public int	rangePort;
		public String	metaIp;
		public int	metaPort;
	}

	public static class cRet
	{
		public long ptr;
		public int length;
		//public int pad;
	}

	public List<Split> mtGetSplits(String tableName)
	{
		cRet ret = new cRet();
		
		getSplits(connectionPtr, tableName, ret);

		long splitsPtr = ret.ptr;
		int splitCount = ret.length;
		
		List<Split> splits = new ArrayList<Split>(splitCount);
		for(int i = 0; i < splitCount; i ++)
		{
			Split split = getSplit(splitsPtr, i);
			splits.add(split);
		}

		freeSplits(splitsPtr);

		return splits;
	}

	public long mtCreateReader(Split split)
	{
		return createReader(split);
	}

	public void mtFreeReader(long readerPtr)
	{
		freeReader(readerPtr);
	}

	public cRet mtGetNextKeyValue(long readerPtr)
	{
		cRet ret = new cRet();

		getNextKeyValue(readerPtr, ret);

		return ret;
	}

	public byte [] mtGetKey(long readerPtr, long rowPtr)
	{
		cRet ret = new cRet();

		getKey(readerPtr, rowPtr, ret);

		long keyPtr = ret.ptr;
		int keyLength = ret.length;

		byte [] keyArray = new byte[keyLength];
		
		arrayCopy(keyArray, keyPtr, keyLength);

		return keyArray;
	}

	public byte[] mtGetValues(long readerPtr, long rowPtr, int rowLength)
        {
		cRet ret = new cRet();

                getValue(readerPtr, rowPtr, rowLength, ret);

                long valuePtr = ret.ptr;
                int valueLength = ret.length;

		//System.out.println("java_length:" + valueLength);

                byte [] valueArray = new byte[valueLength];

                arrayCopy(valueArray, valuePtr, valueLength);

		freeValue(valuePtr);

		return valueArray;
        }

	public static byte[] mtGetValue(byte [] values, int index)
        {
		int start = 4;
		int len, i, j, type;

		for(i = 0; i < index - 1; i ++)
		{
			type = len = 0;
			for (j = 0; j < 4; j++) {
				len += (values[start + j] & 0xFF) << (8 * j);
			}
			type = (len & 0x03);
			len = (len >> 2);
			start += (len + 4);
		}

		type = len = 0;
                for (j = 0; j < 4; j++) {
                        len += (values[start + j] & 0xFF) << (8 * j);
			//System.out.println("java_value:" + values[start + j]);
                }
                type = (len & 0x03);
                len = (len >> 2);
		start += 4;

		//System.out.println("java_type:" + type);
		//System.out.println("java_length:" + len);
                
		byte [] value = new byte[len];
		System.arraycopy(values, start, value, 0, len);

		return value;
        }

}



