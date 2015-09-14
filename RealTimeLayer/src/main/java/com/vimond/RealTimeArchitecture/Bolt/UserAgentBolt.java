package com.vimond.RealTimeArchitecture.Bolt;

import java.io.IOException;
import java.util.Map;

import net.sf.uadetector.OperatingSystem;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.ecyrd.speed4j.StopWatch;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.utils.data.Constants;
import com.vimond.utils.data.StormEvent;
import com.vimond.utils.functions.UserAgentParser;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Bold class in charge of analyzing the user agent of the event in order to
 * retrive information about the browser and the operative system
 * 
 * @author matteoremoluzzi
 *
 */

public class UserAgentBolt implements IRichBolt
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LogManager.getLogger(UserAgentBolt.class);
	private final static Marker ERROR = MarkerManager.getMarker("REALTIME-ERRORS");

	private transient ObjectMapper mapper;
	private transient UserAgentStringParser userAgentParser;
	private OutputCollector collector;
	private boolean acking;
	private StopWatch throughput;


	public UserAgentBolt()
	{
		this.throughput = new StopWatch();
	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
//		this.acking = (Boolean) stormConf.get("acking");
		this.acking = false;
		this.throughput.start();
		this.mapper = new ObjectMapper();
		this.mapper.registerModule(new JodaModule());
		this.userAgentParser = new UserAgentParser();

	}

	public void execute(Tuple input)
	{
		String jsonEvent = input.getString(0);
		long initTime = input.getLong(1);

		StormEvent event = null;
		try
		{
			event = mapper.readValue(jsonEvent, StormEvent.class);
		} catch (JsonParseException e)
		{
		} catch (JsonMappingException e)
		{
		} catch (IOException e)
		{
		}

		if (event != null)
		{
			event.setCounter();
			String userAgentString = event.getUserAgent();

			ReadableUserAgent userAgent = userAgentParser.parse(userAgentString);

			OperatingSystem os = userAgent.getOperatingSystem();

			String os_str = os.getName() + " " + os.getVersionNumber().getMajor();

			// TODO handle new user agent from internet explorer 11

			String browser_str = userAgent.getName() + "/" + userAgent.getVersionNumber().getMajor();

			event.setBrowser(browser_str);
			event.setOs(os_str);

			emit(input, event, initTime);
		}
		// else just skip the tuple. Here we are not interested in high
		// accuracy, we need to process the messages as fast as possible
		else
		{
			LOG.error(ERROR, "Error while processing a tuple");
		}

	}

	public void emit(Tuple input, StormEvent event, long initTime)
	{
		String value = null;
		try
		{
			value = mapper.writeValueAsString(event);
		} catch (JsonProcessingException e)
		{
			LOG.error(ERROR, "Error while converting to JSON String: {}", e.getMessage());
			if (this.acking)
				throw new FailedException(); // i.e. fail on processing the
												// tuple
		}
		if (value != null)
		{
			if (this.acking)
			{
				collector.emit(Constants.UA_STREAM, input, new Values(value, initTime));
				collector.ack(input);
			} else
				collector.emit(Constants.UA_STREAM, new Values(value, initTime));
		}
	}
	
	public void testEmit(Tuple input, StormEvent event, long initTime)
	{
		collector.emit(Constants.UA_STREAM, input, new Values(event, initTime));
	}

	public void cleanup()
	{
		userAgentParser.shutdown();
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
