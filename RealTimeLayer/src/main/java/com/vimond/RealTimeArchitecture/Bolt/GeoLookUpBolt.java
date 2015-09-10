package com.vimond.RealTimeArchitecture.Bolt;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.ecyrd.speed4j.StopWatch;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.vimond.RealTimeArchitecture.Utils.GeoIP;
import com.vimond.RealTimeArchitecture.Utils.GeoInfo;
import com.vimond.utils.data.Constants;
import com.vimond.utils.data.StormEvent;

/**
 * Storm bolt which adds is responsible to inject a country name fields, if
 * found, according to the ip address
 * 
 * @author matteoremoluzzi
 *
 */
public class GeoLookUpBolt implements IRichBolt
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LogManager.getLogger(GeoLookUpBolt.class);
	private final static Marker ERROR = MarkerManager.getMarker("ERRORS");
	private final static Marker THROUGHPUT = MarkerManager.getMarker("PERFORMANCES-THROUGHPUT");
	private static final double FROM_NANOS_TO_MILLIS = 0.0000001;
	
	private int processedTuples;
	private DatabaseReader dbReader;
	private static ObjectMapper mapper;
	private OutputCollector collector;
	private boolean acking;
	private StopWatch throughput;
	
	static {
		mapper = new ObjectMapper();
		mapper.registerModule(new JodaModule());
	}
	
	
	public GeoLookUpBolt()
	{
		this.throughput = new StopWatch();
	}
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		this.acking = Boolean.parseBoolean((String) stormConf.get("acking"));
		this.dbReader = GeoIP.getDbReader();
		this.throughput.start();
	}


	/**
	 * Structure of the input tuple:
	 * <ul>
	 * <li>pos 0 = StormEvent
	 * <li>pos 2 = initTime
	 * </ul>
	 */
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
		
		if(event != null)
		{
			//for aggregation purposes
			event.setCounter();
			
			String ipAddress = event.getIpAddress();
			
			GeoInfo geoInfo = this.getCountryAndCoordinatesFromIp(ipAddress);

			if (geoInfo != null)
			{
				// if(geoInfo.getCountry() != null)
				event.setCountryName(geoInfo.getCountry());
				// if(geoInfo.getCity() != null)
				event.setCity(geoInfo.getCity());
				// if(geoInfo.getLatitute() != null)
				event.setLatitude(geoInfo.getLatitute());
				// if(geoInfo.getLongitude() != null)
				event.setLongitude(geoInfo.getLongitude());
			}

			emit(input, event, initTime, collector);
			if(++processedTuples % Constants.DEFAULT_STORM_BATCH_SIZE == 0)
			{
				this.throughput.stop();
				double avg_throughput = Constants.DEFAULT_STORM_BATCH_SIZE / (this.throughput.getTimeNanos() * FROM_NANOS_TO_MILLIS);
				LOG.info(THROUGHPUT, avg_throughput);
				processedTuples = 0;
				this.throughput.start();
			}
		}
		//else just skip the tuple. Here we are not interested in high accuracy, we need to process the messages as fast as possible
		else
		{
			LOG.error(ERROR, "Error while processing a tuple");
		}
	}

	public void cleanup()
	{
		try
		{
			LOG.warn("Going to sleep now, closing connection with database");
			dbReader.close();
		} catch (IOException e)
		{
			LOG.error("Error while closing the connection with database: {}", e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declareStream(Constants.IP_STREAM, new Fields(Constants.EVENT_MESSAGE, Constants.TIMEINIT));
	}

	/**
	 * Need to serialize the event object for being able to write it on ES as a string. It is not possible to do it in the next bolt!
	 * @param message
	 * @param initTime
	 * @param collector
	 */
	private void emit(Tuple input, StormEvent message, long initTime, OutputCollector collector)
	{
		String value = null;
		try
		{
			value = mapper.writeValueAsString(message);
		} catch (JsonProcessingException e)
		{
			LOG.error(ERROR, "Error while converting to JSON String: {}", e.getMessage());
			if(this.acking)
				throw new FailedException(); // i.e. fail on processing the tuple
		}
		if (value != null)
		{
			if(this.acking)
			{
				collector.emit(Constants.IP_STREAM, input, new Values(value, initTime));
				collector.ack(input);
			}
			else
				collector.emit(Constants.IP_STREAM, new Values(value, initTime));
		}
	}

	/**
	 * Ip address - geoInfo conversion made by MaxMind database
	 * 
	 * @param ipAddress
	 * @return returns the name of the country the <b>ipAddress</b> belongs to,
	 *         <b>null</b> otherwise
	 */
	public GeoInfo getCountryAndCoordinatesFromIp(String ipAddress)
	{
		try{
			InetAddress ipAddr = InetAddress.getByName(ipAddress);
			CityResponse response = this.dbReader.city(ipAddr);
			Country country = response.getCountry();
			Location location = response.getLocation();
			return new GeoInfo(country.getName(), response.getCity().getName(), location.getLatitude(), location.getLongitude());
		}
		catch (UnsupportedOperationException e) //usage of lite version of max mind
		{
			try
			{
				InetAddress ipAddr = InetAddress.getByName(ipAddress);
				CountryResponse response = this.dbReader.country(ipAddr);
				Country country = response.getCountry();
				return new GeoInfo(country.getName());
			} catch (IOException e1)
			{
				
			} catch (GeoIp2Exception e1)
			{
			}
		}
		catch (UnknownHostException e)
		{
	//		LOG.error("UnknownHostException while looking up the ip address");
		} catch (IOException e)
		{
	//		LOG.error("IOException while looking up the ip address");
		} catch (GeoIp2Exception e)
		{
	//		LOG.error("GeoIp2Exception while looking up the ip address");
		}
		return null;
	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}
