package no.vimond.RealTimeArchitecture.Bolt;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import no.vimond.RealTimeArchitecture.Utils.Constants;
import no.vimond.RealTimeArchitecture.Utils.GeoIP;
import no.vimond.RealTimeArchitecture.Utils.GeoInfo;
import no.vimond.RealTimeArchitecture.Utils.StormEvent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import com.vimond.common.shared.ObjectMapperConfiguration;

/**
 * Storm bolt which adds is responsible to inject a country name fields, if
 * found, according to the ip address
 * 
 * @author matteoremoluzzi
 *
 */
public class GeoLookUpBolt extends BaseBasicBolt
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LogManager.getLogger(GeoLookUpBolt.class);
	
	private DatabaseReader dbReader;
	private ObjectMapper mapper;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context)
	{
		this.dbReader = GeoIP.getDbReader();
		this.mapper = new ObjectMapper();
		this.mapper.registerModule(new JodaModule());
	}

	/**
	 * Structure of the input tuple:
	 * <ul>
	 * <li>pos 0 = StormEvent
	 * <li>pos 1 = Ip address
	 * </ul>
	 */
	public void execute(Tuple input, BasicOutputCollector collector)
	{
		StormEvent message = (StormEvent) input.getValue(0);
		String ipAddress = (String) input.getValue(1);

		GeoInfo geoInfo = this.getCountryAndCoordinatesFromIp(ipAddress);

		if (geoInfo != null)
		{
			// if(geoInfo.getCountry() != null)
			message.setCountryName(geoInfo.getCountry());
			// if(geoInfo.getCity() != null)
			message.setCity(geoInfo.getCity());
			// if(geoInfo.getLatitute() != null)
			message.setLatitude(geoInfo.getLatitute());
			// if(geoInfo.getLongitude() != null)
			message.setLongitude(geoInfo.getLongitude());
		}

		emit(message, collector);
	}

	public void cleanup()
	{
		try
		{
			LOG.warn("Going to sleep now, closing connection with database");
			dbReader.close();
		} catch (IOException e)
		{
			LOG.error("Error while closing the connection with database");
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declareStream(Constants.IP_STREAM, new Fields(
				Constants.EVENT_MESSAGE, "initTime"));
	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}

	private void emit(StormEvent message, BasicOutputCollector collector)
	{
		String value = null;
		long initTime = message.getInitTime();
		try
		{
			value = this.mapper.writeValueAsString(message);
		} catch (JsonProcessingException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (value != null)
		{
			collector.emit(Constants.IP_STREAM, new Values(value, initTime));
		} else
			throw new FailedException(); // i.e. fail on processing tuple
		LOG.info("Processed message");
		LOG.info("Received message with init time : " + initTime);
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
		try
		{
			InetAddress ipAddr = InetAddress.getByName(ipAddress);

			CountryResponse response = this.dbReader.country(ipAddr);
			Country country = response.getCountry();
			// Location location = response.getLocation();

			return new GeoInfo(country.getName());

		} catch (UnknownHostException e)
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
}
