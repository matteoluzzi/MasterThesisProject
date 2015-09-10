package com.vimond.RealTimeArchitecture.Kafka;

import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Schema for deserialization from kafka to storm. From byte array to string plus a timestamp for monitoring
 * @author matteoremoluzzi
 *
 */
public class StormEventSchema implements Scheme
{

	private static final long serialVersionUID = -5279339384920260687L;
	
	public StormEventSchema()
	{
	}
	
	public List<Object> deserialize(byte[] ser)
	{
		String e = new String(ser);
		long timestamp = System.nanoTime();
		
		return new Values(e, timestamp);
	}

	public Fields getOutputFields()
	{
		return new Fields("event", "initTime");
	}

}
