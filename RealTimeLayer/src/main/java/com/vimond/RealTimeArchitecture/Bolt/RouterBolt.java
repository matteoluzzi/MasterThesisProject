package com.vimond.RealTimeArchitecture.Bolt;

import java.io.File;
import java.util.Locale;
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

import com.ecyrd.speed4j.StopWatch;
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
	private StopWatch throughput;
	private boolean acking;
	private int processedTuples;

	private final static Marker THROUGHPUT = MarkerManager.getMarker("PERFORMANCES-REALTIME-THROUGHPUT");

	private static final double FROM_NANOS_TO_SECONDS = 0.000000001;

	public RouterBolt()
	{
		this.throughput = new StopWatch();
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
//		this.acking = (Boolean) stormConf.get("acking");
		this.acking = false;
		this.collector = collector;
		this.processedTuples = 0;
		this.throughput.start();
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
		
		String message = input.getString(0);
		long initTime = input.getLong(1);

		if (message != null)
		{
			// look for occurrences of ipAddress string, faster than deserialize
			// the object and check for the field

			int userAgent_index = message.indexOf("userAgent");

			if (userAgent_index != -1)
			{
				emitOnUAStream(input, message, initTime);
			}
			
			// get statistics on current batch
//			if (++processedTuples % Constants.DEFAULT_STORM_BATCH_SIZE == 0)
//			{
//				this.throughput.stop();
//				double avg_throughput = Constants.DEFAULT_STORM_BATCH_SIZE / (this.throughput.getTimeNanos() * FROM_NANOS_TO_SECONDS);
//				LOG.info(THROUGHPUT, avg_throughput);
//				processedTuples = 0;
//				this.throughput.start();
//			}
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
