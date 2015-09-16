package com.vimond.RealTimeArchitecture.Bolt;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.utils.data.Constants;
import com.vimond.utils.data.StormEvent;

public class SerializerBolt implements IRichBolt
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LogManager.getLogger(SerializerBolt.class);

	private transient ObjectMapper mapper;

	private OutputCollector collector;
	private boolean acking;
	private long reportFrequency;
	private String reportPath;
	
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		this.acking = false;
		this.mapper = new ObjectMapper();
		this.mapper.registerModule(new JodaModule());
		
	}
	public void execute(Tuple input)
	{
		StormEvent event = (StormEvent) input.getValue(0);
		long initTime = input.getLong(1);
		
		String jsonEvent = null;
		try
		{
			jsonEvent = this.mapper.writeValueAsString(event);
		} catch (JsonProcessingException e)
		{
			
		}
		emit(input, jsonEvent, initTime);
	}
	
	public void emit(Tuple input, String event, long initTime)
	{

		if (this.acking)
		{
			collector.emit(Constants.UA_STREAM, input, new Values(event, initTime));
			collector.ack(input);
		} else
			collector.emit(Constants.UA_STREAM, new Values(event, initTime));
	}
	public void cleanup()
	{
		
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declareStream(Constants.UA_STREAM, new Fields("event", "initTime"));
		
	}
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}
