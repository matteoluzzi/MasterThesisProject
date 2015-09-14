package com.vimond.eventfetcher;

import java.io.IOException;
import java.util.Arrays;

import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.vimond.pailStructure.TimeFramePailStructure;

public class EventFetcherTest extends TestCase
{
	public EventFetcherTest(String testName)
	{
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite(EventFetcherTest.class);
	}
	
	public void testFolder() throws JsonParseException, JsonMappingException, IOException
	{
		String message = "{\"eventName\":\"user-asset_playback-event\",\"originator\":\"ibc-api\",\"versions\":[\"1.0\"],\"timestamp\":\"2014-09-12T08:42:05.773Z\",\"guid\":\"dcf7760d-4f97-490c-9ea6-a47fe8d8217b\",\"ipAddress\":\"82.134.26.130\",\"asset_playback\":{\"assetId\":542,\"userId\":-1,\"orderId\":null,\"categoryId\":1215,\"categoryPlatformId\":375,\"ip\":\"82.134.26.130\",\"bandwidth\":0,\"geo.organization\":\"Unknown\",\"geo.isp\":\"Unknown\",\"geo.country\":\"Unknown\",\"videoFileId\":2509,\"playType\":\"ONDEMAND\",\"ispName\":null,\"referrer\":null,\"appName\":\"VTV-HTML\"}}";
		TimeFramePailStructure structure = new TimeFramePailStructure();
		TimeFramePailStructure.initialize(180);
		
		List<String> folder =  structure.getTarget(message);
		
		List<String> expected = Arrays.asList(new String[] {"2014-09-12", "03", "00"});
		
		assertEquals(expected, folder);
	}
	
	public void testReport()
	{
		MetricRegistry registry = new MetricRegistry();
		
		Meter meter = registry.meter("test-meter");
		
		ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
									.convertRatesTo(TimeUnit.SECONDS)
									.convertDurationsTo(TimeUnit.MILLISECONDS)
									.build();
		
		for(int i=0; i< 100; i++)
		{
			meter.mark();
		}
		
		reporter.report();
		
	}
}
