package org.maxtable.examples;

import java.io.*;
import java.net.*;
import java.util.Random;
import java.util.Date;
import java.nio.ByteBuffer;

import org.maxtable.client.MtAccess;

public class JClientTest
{
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java JClientTest opt_type");
            System.exit(1);
        }
        
        try {
        	MtAccess.mtCreateContext();
		//System.exit(1);        	
            MtAccess mtAccess = new MtAccess("172.16.10.42", 1959);

            long exePtr;

            if(args[0].equals("create"))
            {
                String cmd = "create table gu(id1 varchar, id2 varchar,id3 int)";

                exePtr = mtAccess.mtOpenExecute(cmd);

                if (exePtr == 0) {
            		throw new IOException("Unable to open mt execute");
        		}

                mtAccess.mtCloseExecute(exePtr);
            }

            if(args[0].equals("select"))
            {
                String cmd = "select gu(aaaa)";

                exePtr = mtAccess.mtOpenExecute(cmd);

                if (exePtr == 0) {
            		throw new IOException("Unable to open mt execute");
        		}

        		long rp = mtAccess.mtGetNextrow(exePtr);
			System.out.println(rp);
			//System.out.println(rp.length());

        		if(rp>0)
        		{
        			MtAccess.ColValue col;
        			col = mtAccess.mtGetColValue(exePtr, rp, 0);
        			System.out.println("col 0 value:" + col.value);
        			col = mtAccess.mtGetColValue(exePtr, rp, 1);
        			System.out.println("col 1 value:" + col.value);
        			col = mtAccess.mtGetColValue(exePtr, rp, 2);
        			System.out.println("col 2 value:" + col.value);
        		}

                mtAccess.mtCloseExecute(exePtr);
            }

            if(args[0].equals("insert"))
            {
                String cmd = "insert into gu(aaaa, bbbb, 1111)";

                exePtr = mtAccess.mtOpenExecute(cmd);

                if (exePtr == 0) {
            		throw new IOException("Unable to open mt execute");
        		}

                mtAccess.mtCloseExecute(exePtr);
            }
            
            System.out.println("Test passed!");

			mtAccess.mtCloseConnection();
            MtAccess.mtDestroyContext();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unable to setup MtAccess");
            System.exit(1);
        }
    }

}

