package com.turbulence.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

public class GetRowKeysAll {
	static ArrayList<String> keys = new ArrayList<String>();
	
	public static ArrayList<String> retreiveRows(){
	    try {
	    	TFramedTransport transport = new TFramedTransport(new TSocket("localhost", 9160));
	        TProtocol protocol = new TBinaryProtocol(transport);
	        Client client = new Cassandra.Client(protocol);
	        transport.open();
	        client.set_keyspace("Turbulence");
	        ColumnParent columnParent = new ColumnParent("InstanceData"); 
	        SlicePredicate predicate = new SlicePredicate();
	        predicate.setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]), false, 1));              
	        KeyRange keyRange = new KeyRange();  //Get all keys
	        keyRange.setStart_key(new byte[0]);
	        keyRange.setEnd_key(new byte[0]);
	        long start = System.currentTimeMillis();
	        List<KeySlice> keySlices = client.get_range_slices(columnParent, predicate, keyRange, ConsistencyLevel.ONE);
	        ArrayList<Integer> list = new ArrayList<Integer>();
	        for (KeySlice ks : keySlices) {
	                 keys.add(ByteBuffer.wrap(ks.getKey()).toString());
	        }    
	        Collections.sort(list);
	        System.out.println((System.currentTimeMillis()-start));
	        for(Integer i: list){
	            System.out.println(i);
	        }

	        transport.close();
	        
	    } catch (Exception e) {
	        e.printStackTrace();
	        

	    }
		return keys;
	}

}
