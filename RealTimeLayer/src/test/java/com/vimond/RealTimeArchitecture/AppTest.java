package com.vimond.RealTimeArchitecture;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

import org.joda.time.DateTime;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.utils.data.StormEvent;

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
	private static final String message = "{\"eventName\":\"user-asset_playback-event\",\"originator\":\"ibc-api\",\"versions\":[\"1.0\"],\"timestamp\":\"2014-09-12T08:42:05.773Z\",\"guid\":\"dcf7760d-4f97-490c-9ea6-a47fe8d8217b\",\"ipAddress\":\"82.134.26.130\",\"asset_playback\":{\"assetId\":542,\"userId\":-1,\"orderId\":null,\"categoryId\":1215,\"categoryPlatformId\":375,\"ip\":\"82.134.26.130\",\"bandwidth\":0,\"geo.organization\":\"Unknown\",\"geo.isp\":\"Unknown\",\"geo.country\":\"Unknown\",\"videoFileId\":2509,\"playType\":\"ONDEMAND\",\"ispName\":null,\"referrer\":null,\"appName\":\"VTV-HTML\"}}";

	public AppTest(String testName)
	{
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite(AppTest.class);
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp()
	{
		assertTrue(true);
	}

	public void testConversion() throws JsonParseException, JsonMappingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new JodaModule());
		StormEvent e = mapper.readValue(message, StormEvent.class);
		assertNotNull(e);
	}

	public void testUserAgent() throws IOException
	{
		UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();

		final String oldUA = "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; FunWebProducts; BOIE9;ENUSMSCOM; rv:11.0) like Gecko";
		final String newUA = "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; MASMJS; rv:11.0) like Gecko";

		ReadableUserAgent agent = parser.parse(newUA);

		String browser = agent.getName() + "/" + agent.getVersionNumber().getMajor();
		String os = agent.getOperatingSystem().getName() + " " + agent.getOperatingSystem().getVersionNumber().getMajor();
		//will fail due to a bug of the library
		//assertEquals("IE/11", browser);
		assertEquals("Windows 10", os);
		
		agent = parser.parse(oldUA);
		
		browser = agent.getName() + "/" + agent.getVersionNumber().getMajor();
		os = agent.getOperatingSystem().getName();
		assertEquals("IE/11", browser);
		assertEquals("Windows 7", os);
	}
}
