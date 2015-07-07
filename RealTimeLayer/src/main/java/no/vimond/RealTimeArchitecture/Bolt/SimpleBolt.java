package no.vimond.RealTimeArchitecture.Bolt;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import no.vimond.RealTimeArchitecture.Utils.Constants;
import no.vimond.RealTimeArchitecture.Utils.StormEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class SimpleBolt extends BaseBasicBolt
{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(SimpleBolt.class);

	private AtomicInteger count;
	private ObjectMapper mapper;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context)
	{
		this.count = new AtomicInteger(0);
		this.mapper = ObjectMapperConfiguration.configure();
	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}

	public void cleanup()
	{
		LOG.warn("Going to sleep now");
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(Constants.EVENT_MESSAGE));
		declarer.declareStream(Constants.IP_STREAM, new Fields(Constants.EVENT_MESSAGE, Constants.IP_MESSAGE));
	}
	
	public void execute(Tuple input, BasicOutputCollector collector)
	{
		StormEvent message = (StormEvent) input.getValue(0);
		LOG.info("Received message " + this.count.getAndIncrement() + " "
				+ message);
		
		String ipAddress = message.getIpAddress();
		
		if(ipAddress == null) //emit on default stream
		{
			emitOnDefaultStream(message, collector);
		}
		else //emit to ip_stream for geolocation analysis
		{
			emitOnIpStream(message, ipAddress, collector);
		}
	}
	
	private void emitOnIpStream(StormEvent message, String ipAddress, BasicOutputCollector collector)
	{
		collector.emit(Constants.IP_STREAM, new Values(message, ipAddress));
	}
	
	private void emitOnDefaultStream(StormEvent message, BasicOutputCollector collector)
	{
		String value = null;
		try
		{
			 value = mapper.writeValueAsString(message);
		} catch (JsonProcessingException e)
		{
			LOG.warn("Error while converting StormEvent into a JSON string");
		}
		if(value != null)
			collector.emit(new Values(value));
		else
			throw new FailedException(); //i.e. fail on processing tuple
	}
}