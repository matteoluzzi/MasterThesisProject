package no.vimond.RealTimeArchitecture.Kafka;

import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StormEventSchema implements Scheme
{

	private static final long serialVersionUID = -5279339384920260687L;
	//private JsonDecoder<StormEvent> jsonDecoder;
	
	public StormEventSchema()
	{
		//this.jsonDecoder= new JsonDecoder<StormEvent>(StormEvent.class, ObjectMapperConfiguration.configurePretty());
	}
	
	public List<Object> deserialize(byte[] ser)
	{
		String e = new String(ser);
		
		return new Values(e);
	}

	public Fields getOutputFields()
	{
		return new Fields("event");
	}

}
