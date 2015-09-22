package com.vimond.eventfetcher.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingQueue;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerService;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.eventfetcher.configuration.ProcessorConfiguration;
import com.vimond.eventfetcher.processor.BatchProcessor;
import com.vimond.eventfetcher.util.Constants;
import com.vimond.eventfetcher.writer.Writer;
import com.vimond.pailStructure.TimeFrameCurrentTimePailStructure;
import com.vimond.pailStructure.TimeFramePailStructure;

/**
 * Unrealiable version of kafka consumer. It commits the offset after reading
 * the message from the broker.<b> The flushing on HDFS is done either after a
 * timeframe by a TimerTask or after a fixed number of messages of the buffer.
 * put and drainTo methods are thread-safe. Usage of internal locking mechanism
 * 
 * @author matteoremoluzzi
 *
 */
public class UnreliableKafkaConsumerService<T> extends KafkaConsumerService<T> implements KafkaConsumerEventFetcher<T>
{
	private LinkedBlockingQueue<T> buffer;
	private List<T> flush_buffer;
	private Timer flushTimer;

	private long flushingTime;
	private long batchSize;
	private String HDFSPathToLocation;
	private int timeFrameInMinutes;
	private String pailStructureType;

	@SuppressWarnings({ "unchecked" })
	public UnreliableKafkaConsumerService(MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig, KafkaConsumerConfig consumerConfig, BatchProcessor fsProcessor, ProcessorConfiguration conf)
	{
		super(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, (MessageProcessor<T>) fsProcessor);
		// bind message processor to this
		fsProcessor.setEventsKafkaConsumer(this);
		this.flushTimer = new Timer();
		this.buffer = new LinkedBlockingQueue<T>();
		this.flush_buffer = new ArrayList<T>();
		this.flushingTime = conf.getConfig().get(Constants.FLUSHING_TIME_KEY) != null ? Long.parseLong(conf.getConfig().get(Constants.FLUSHING_TIME_KEY)) : Constants.DEFAULT_FLUSH_TIME;
		this.batchSize = conf.getConfig().get(Constants.MAX_MESSAGES_KEY) != null ? Long.parseLong(conf.getConfig().get(Constants.MAX_MESSAGES_KEY)) : Constants.DEFAULT_MAX_MESSAGES_INTO_FILE;
		this.HDFSPathToLocation = conf.getConfig().get(Constants.HDFS_PATH_TO_LOCATION_KEY) != null ? conf.getConfig().get(Constants.HDFS_PATH_TO_LOCATION_KEY) : Constants.DEFAULT_HDFS_PATH_TO_LOCATION;
		this.timeFrameInMinutes = conf.getConfig().get(Constants.TIME_FRAME_KEY) != null ? Integer.parseInt(conf.getConfig().get(Constants.TIME_FRAME_KEY)) : Constants.DEFAULT_TIME_FRAME;
		this.pailStructureType = conf.getConfig().get("pailStructureType") != null ? conf.getConfig().get("pailStructureType") : Constants.DEFAULT_TIME_FRAME_TYPE;

		this.initializeTimeFramePail(this.pailStructureType);

		this.setUpTimerTask();
		// procedure for controlled shutdown
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			public void run()
			{
				shutdown();
			}
		});
	}

	public LinkedBlockingQueue<T> getBuffer()
	{
		return buffer;
	}

	public int getTimeFrameInMinutes()
	{
		return timeFrameInMinutes;
	}

	public String getHDFSPathToLocation()
	{
		return HDFSPathToLocation;
	}

	public void putMessageIntoBuffer(T message)
	{
		try
		{
			this.buffer.put(message);
		} catch (InterruptedException e)
		{
		}
		if (this.buffer.size() >= this.batchSize)
		{
			synchronized (this)
			{
				if (this.buffer.size() >= this.batchSize)
				{
					// delete timertaks and launch it now
					this.deleteTimerTask();
					copyMessagesOnFlushArray();
					new Writer<T>(this).run();
					this.setUpTimerTask();
				}
			}
		}
	}

	public void setUpTimerTask()
	{
		this.flushTimer = new Timer();
		this.flushTimer.scheduleAtFixedRate(new Writer<T>(this), this.flushingTime, this.flushingTime);
	}

	public void deleteTimerTask()
	{
		this.flushTimer.cancel();
	}

	public List<T> getMessages()
	{
		return flush_buffer;
	}

	public synchronized void copyMessagesOnFlushArray()
	{
		if (this.buffer.size() > 0)
			this.buffer.drainTo(flush_buffer);
	}

	public void shutdown()
	{
		this.executor.shutdown();
		// flush the buffer to disk
		new Writer<T>(this).run();
	}

	public void initializeTimeFramePail(String type)
	{
		switch (type)
		{
		case "timestamp":
			TimeFramePailStructure.initialize(getTimeFrameInMinutes());
			break;
		case "current":
			TimeFrameCurrentTimePailStructure.initialize(getTimeFrameInMinutes());
			break;
		}
		
	}
}
