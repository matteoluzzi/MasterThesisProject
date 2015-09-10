package com.vimond.RealTimeArchitecture.Bolt;

import java.io.File;
import java.util.Map;
import java.util.UUID;
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
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.vimond.utils.data.Constants;

/**
 * Simple bolt which acts as a tuple router according to the useragent field in
 * the message
 * 
 * @author matteoremoluzzi
 *
 */
public class RouterBolt implements IRichBolt
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LogManager.getLogger(RouterBolt.class);
	private OutputCollector collector;
	private boolean acking;
	private long reportFrequency;
	private String reportPath;
	
	private transient Timer timer;
	private transient Meter counter;

	public RouterBolt()
	{
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.acking = (Boolean) stormConf.get("acking");
		this.collector = collector;
		this.reportFrequency = (Long) stormConf.get("metric.report.interval");
		this.reportPath = (String) stormConf.get("metric.report.path");
		initializeMetricsReport();
	}
	
	public void initializeMetricsReport()
	{
		final MetricRegistry metricRegister = new MetricRegistry();	
		
		//use sampling when detecting the latency for not affecting the performances
		this.timer = new Timer(new UniformReservoir());
		metricRegister.register(MetricRegistry.name(RouterBolt.class, Thread.currentThread().getName() + "latency"), this.timer);
		
		//register the meter metric
		this.counter = metricRegister.meter(MetricRegistry.name(RouterBolt.class, Thread.currentThread().getName() + "-events_sec"));
		
		final CsvReporter reporter = CsvReporter.forRegistry(metricRegister)
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.NANOSECONDS)
				.build(new File(this.reportPath));
		reporter.start(this.reportFrequency, TimeUnit.SECONDS);
		
	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}

	public void cleanup()
	{
		LOG.debug("Going to sleep now");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(Constants.EVENT_MESSAGE, Constants.TIMEINIT));
		declarer.declareStream(Constants.IP_STREAM, new Fields(Constants.EVENT_MESSAGE, Constants.TIMEINIT));
		declarer.declareStream(Constants.UA_STREAM, new Fields(Constants.EVENT_MESSAGE, Constants.TIMEINIT));
	}

	public void execute(Tuple input)
	{
		this.counter.mark();
		final Timer.Context ctx = this.timer.time();
		String message = input.getString(0);
		long initTime = input.getLong(1);

		if (message != null)
		{
			// look for occurrences of ipAddress string, faster than deserialize
			// the object and check for the field

			int userAgent_index = message.indexOf("userAgent");

			if (userAgent_index == -1)
			{
				emitOnDefaultStream(input, message, initTime);
			}

			else
			{
				emitOnUAStream(input, message, initTime);
			}
			ctx.stop();
		}
	}

	private void emitOnUAStream(Tuple input, String message, long initTime)
	{
		if (this.acking)
		{
			this.collector.emit(Constants.UA_STREAM, input, new Values(message, initTime));
			this.collector.ack(input);

		} else
			this.collector.emit(Constants.UA_STREAM, new Values(message, initTime));
	}

	private void emitOnDefaultStream(Tuple input, String message, long initTime)
	{
		if (this.acking)
		{
			this.collector.emit(input, new Values(message, initTime));
			this.collector.ack(input);
		} else
			this.collector.emit(new Values(message, initTime));
	}
}
