package com.vimond.RealTimeArchitecture.Bolt;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.sf.uadetector.OperatingSystem;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.utils.data.Constants;
import com.vimond.utils.data.StormEvent;
import com.vimond.utils.functions.UserAgentParser;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
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
	private long reportFrequency;
	private String reportPath;
	
	private transient Meter counter;

	public UserAgentBolt()
	{
	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		// this.acking = (Boolean) stormConf.get("acking");
		this.acking = false;
		this.mapper = new ObjectMapper();
		this.mapper.registerModule(new JodaModule());
		this.userAgentParser = new UserAgentParser();
		this.acking = (Boolean) stormConf.get("acking");
		this.reportFrequency = (Long) stormConf.get("metric.report.interval");
		this.reportPath = (String) stormConf.get("metric.report.path");
		initializeMetricsReport();

	}
	
	public void initializeMetricsReport()
	{
		final MetricRegistry metricRegister = new MetricRegistry();	
		
		//register the meter metric
		this.counter = metricRegister.meter(MetricRegistry.name(UserAgentBolt.class, Thread.currentThread().getName() + "-events_sec"));
		
		final CsvReporter reporter = CsvReporter.forRegistry(metricRegister)
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.NANOSECONDS)
				.build(new File(this.reportPath));
		reporter.start(this.reportFrequency, TimeUnit.SECONDS);
		
	}

	public void execute(Tuple input)
	{
		this.counter.mark();
		
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

		if (this.acking)
		{
			collector.emit(Constants.UA_STREAM, input, new Values(event, initTime));
			collector.ack(input);
		} else
			collector.emit(Constants.UA_STREAM, new Values(event, initTime));
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
