package no.vimond.RealTimeArchitecture.Utils;

import java.io.IOException;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class EventTest extends TestCase
{
	private ObjectMapper objMapper;
	
	@Before
	public void initialize()
	{
		objMapper = ObjectMapperConfiguration.configurePretty();
	}
	
	public EventTest( String testName )
    {
        super( testName );
    }
	
	@Test
	public void testConversion()
	{
		objMapper = ObjectMapperConfiguration.configure();
		String JSONMessage = "{\"eventName\":\"user-asset_playback-event\",\"originator\":\"ibc-api\",\"tenant\":null,\"originatorId\":null,\"versions\":[\"1.0\"],\"timestamp\":\"2014-09-16T03:20:35.622Z\",\"guid\":\"691a2e12-7d98-4712-8bac-5eb3a1753d66\",\"asset_playback\":{\"assetId\":515,\"userId\":-1,\"orderId\":null,\"categoryId\":1215,\"categoryPlatformId\":375,\"ip\":\"175.136.252.13\",\"bandwidth\":0,\"geo.organization\":\"Unknown\",\"geo.isp\":\"Unknown\",\"geo.country\":\"Unknown\",\"videoFileId\":2138,\"playType\":\"ONDEMAND\",\"ispName\":null,\"referrer\":null,\"appName\":\"TV 2 Sumo Silverlight\"}}";
		try
		{
			StormEvent event = objMapper.readValue(JSONMessage, StormEvent.class);
			assertNotNull(event);
			
			String json = ObjectMapperConfiguration.configure().writeValueAsString(event);
			assertEquals(JSONMessage, json);
		} catch (JsonParseException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
