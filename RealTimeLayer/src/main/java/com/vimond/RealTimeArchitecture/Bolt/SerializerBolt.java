package com.vimond.RealTimeArchitecture.Bolt;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.utils.data.Constants;
import com.vimond.utils.data.StormEvent;

/**
 * Bolt which serializes a StormEvent into a json string for a better elasticsearch mapping
 * @author matteoremoluzzi
 *
 */
public class SerializerBolt implements IRichBolt
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LogManager.getLogger(SerializerBolt.class);
	private final static Marker ERROR = MarkerManager.getMarker("REALTIME-ERRORS");

	private transient ObjectMapper mapper;

	private OutputCollector collector;
	private boolean acking;
	private long reportFrequency;
	private String reportPath;
	private transient Meter counter;
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		this.acking = false;
		this.mapper = new ObjectMapper();
		this.mapper.registerModule(new JodaModule());
		this.reportFrequency = (Long) stormConf.get("metric.report.interval");
		this.reportPath = (String) stormConf.get("metric.report.path");
		initializeMetricsReport();

	}

	public void initializeMetricsReport()
	{
		final MetricRegistry metricRegister = new MetricRegistry();

		// register the meter metric
		this.counter = metricRegister.meter(MetricRegistry.name(SerializerBolt.class, Thread.currentThread().getName() + "-events_sec"));

		final CsvReporter reporter = CsvReporter.forRegistry(metricRegister).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.NANOSECONDS).build(new File(this.reportPath));
		reporter.start(this.reportFrequency, TimeUnit.SECONDS);

	}
	
	public void execute(Tuple input)
	{
		this.counter.mark();
		
		StormEvent event = (StormEvent) input.getValue(0);
		long initTime = input.getLong(1);
		
		String jsonEvent = null;
		try
		{
			jsonEvent = this.mapper.writeValueAsString(event);
		} catch (JsonProcessingException e)
		{
			LOG.error(ERROR, "Error while serializing a StormEvent into a json String, skip it...");
		}
		emit(input, jsonEvent, initTime);
	}
	
	public void emit(Tuple input, String event, long initTime)
	{

		if (this.acking)
		{
			collector.emit(Constants.UA_STREAM, input, new Values(event, initTime));
			collector.ack(input);
		} else
			collector.emit(Constants.UA_STREAM, new Values(event, initTime));
	}
	
	public void cleanup()
	{
		LOG.info("Going to shutdown!");
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
