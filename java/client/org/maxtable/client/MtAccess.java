
package org.maxtable.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.io.ByteArrayInputStream;

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


    static {
        try {
            System.loadLibrary("mt_access");
        } catch (UnsatisfiedLinkError e) {
            e.printStackTrace();
            System.err.println("Unable to load mt_access native library: " + System.getProperty("java.library.path"));
            System.exit(1);
        }
    }

    public MtAccess(String metaServerHost, int metaServerPort) throws IOException
    {
        connectionPtr = openConnection(metaServerHost, metaServerPort);
        if (connectionPtr == 0) {
            throw new IOException("Unable to initialize maxtable java client");
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


}



