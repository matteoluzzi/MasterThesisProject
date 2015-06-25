package no.vimond.RealTimeArchitecture.Bolt;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import no.vimond.RealTimeArchitecture.Utils.Constants;
import no.vimond.RealTimeArchitecture.Utils.GeoIP;
import no.vimond.RealTimeArchitecture.Utils.GeoInfo;
import no.vimond.RealTimeArchitecture.Utils.StormEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.vimond.common.shared.ObjectMapperConfiguration;
/**
 * Storm bolt which adds is responsible to inject a country name fields, if found, according to the ip address
 * @author matteoremoluzzi
 *
 */
public class GeoLookUpBolt implements IRichBolt
{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(GeoLookUpBolt.class);

	private DatabaseReader dbReader;
	private OutputCollector collector;
	private ObjectMapper mapper;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector)
	{
		this.dbReader = GeoIP.getDbReader();
		this.collector = collector;
		this.mapper = ObjectMapperConfiguration.configure();
	}
	
	/**
	 * Structure of the input tuple:
	 * <ul>
	 * 	<li>pos 0 = StormEvent
	 * 	<li>pos 1 = Ip address
	 * </ul>
	 */
	public void execute(Tuple input)
	{
		StormEvent message = (StormEvent) input.getValue(0);
		String ipAddress = (String) input.getValue(1);
		
		GeoInfo geoInfo = this.getCountryAndCoordinatesFromIp(ipAddress);
		
		if(geoInfo != null)
		{
			if(geoInfo.getCountry() != null)
				message.setCountryName(geoInfo.getCountry());
			if(geoInfo.getCity() != null)
				message.setCity(geoInfo.getCity());
			if(geoInfo.getLatitute() != null)
				message.setLatitude(geoInfo.getLatitute());
			if(geoInfo.getLongitude() != null)
				message.setLongitude(geoInfo.getLongitude());
		}
						
		emit(Constants.IP_STREAM, message, input);
	}

	public void cleanup()
	{
		try
		{
			LOG.info("Going to sleep now, closing connection with database");
			dbReader.close();
		} catch (IOException e)
		{
			LOG.error("Error while closing the connection with database");
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declareStream(Constants.IP_STREAM, new Fields(Constants.EVENT_MESSAGE));
	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
	
	private void emit(String stream, StormEvent message, Tuple input)
	{
		String value = null;
		try
		{
			 value = this.mapper.writeValueAsString(message);
		} catch (JsonProcessingException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(value != null)
			collector.emit(stream, new Values(value));
		else
			collector.fail(input);
	}

	/**
	 * Ip address - geoInfo conversion made by MaxMind database
	 * @param ipAddress
	 * @return returns the name of the country the <b>ipAddress</b> belongs to, <b>null</b> otherwise
	 */
	public GeoInfo getCountryAndCoordinatesFromIp(String ipAddress)
	{
		Object[] ret = new Object[3];
		try
		{
			InetAddress ipAddr = InetAddress.getByName(ipAddress);

			CountryResponse response = this.dbReader.country(ipAddr);
			Country country = response.getCountry();
			//Location location = response.getLocation();
			
			return new GeoInfo(country.getIsoCode());

		} catch (UnknownHostException e)
		{
			LOG.error("UnknownHostException while looking up the ip address");
		} catch (IOException e)
		{
			LOG.error("IOException while looking up the ip address");
		} catch (GeoIp2Exception e)
		{
			LOG.error("GeoIp2Exception while looking up the ip address");
		}
		return null;
	}

}
