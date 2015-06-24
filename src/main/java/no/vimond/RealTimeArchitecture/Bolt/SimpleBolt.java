package no.vimond.RealTimeArchitecture.Bolt;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import no.vimond.RealTimeArchitecture.Utils.Constants;
import no.vimond.RealTimeArchitecture.Utils.StormEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.shared.ObjectMapperConfiguration;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SimpleBolt implements IRichBolt
{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(SimpleBolt.class);

	private AtomicInteger count;
	private OutputCollector collector;
	private ObjectMapper mapper;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector)
	{
		this.count = new AtomicInteger(0);
		this.collector = collector;
		this.mapper = ObjectMapperConfiguration.configure();
	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}

	public void execute(Tuple input)
	{
		StormEvent message = (StormEvent) input.getValue(0);
		LOG.info("Received message " + this.count.getAndIncrement() + " "
				+ message);
		
		String ipAddress = message.getIpAddress();
		
		if(ipAddress == null) //emit on default stream
		{
			emitOnDefaultStream(message, input);
		}
		else //emit to ip_stream for geolocation analysis
		{
			emitOnIpStream(message, ipAddress);
		}
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
	
	private void emitOnIpStream(StormEvent message, String ipAddress)
	{
		this.collector.emit(Constants.IP_STREAM, new Values(message, ipAddress));
	}
	
	private void emitOnDefaultStream(StormEvent message, Tuple input)
	{
		String value = null;
		try
		{
			 value = mapper.writeValueAsString(message);
		} catch (JsonProcessingException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(value != null)
			collector.emit(new Values(value));
		else
			collector.fail(input);
	}


}
