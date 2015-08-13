package con.vimond.eventfetcher.consumer;

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
import com.vimond.eventfetcher.ProcessorConfiguration;

import con.vimond.eventfetcher.processor.BatchProcessor;
import con.vimond.eventfetcher.util.Constants;
import con.vimond.eventfetcher.writer.HDFSWriter;

/**
 * Unrealiable version of kafka consumer. It commits the offset after reading the message from the broker.<b>
 * The flushing on HDFS is done either after a timeframe by a TimerTask or after a fixed number of messages of the buffer.
 * put and drainTo methods are thread-safe. Usage of internal locking mechanism
 * @author matteoremoluzzi
 *
 */
public class UnreliableKafkaConsumerGroup<T> extends KafkaConsumerService<T> implements KafkaConsumerEventFetcher<T>
{
	private LinkedBlockingQueue<Object> buffer;
	private List<Object> flush_buffer;
	private Timer flushTimer;

	private long flushingTime;
	private long maxMessageIntoFile;
	private String pathToLocation;
	private int timeFrameInMinutes;

	@SuppressWarnings({ "unchecked" })
	public UnreliableKafkaConsumerGroup(MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig, KafkaConsumerConfig consumerConfig, BatchProcessor fsProcessor, ProcessorConfiguration conf)
	{
		super(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, (MessageProcessor<T>) fsProcessor);
		// bind message processor to this
		fsProcessor.setEventsKafkaConsumer(this);
		this.flushTimer = new Timer();
		this.buffer = new LinkedBlockingQueue<Object>();
		this.flush_buffer = new ArrayList<Object>();
		this.flushingTime = conf.getConfig().get(Constants.FLUSHING_TIME_KEY) != null ? Long.parseLong(conf.getConfig().get(Constants.FLUSHING_TIME_KEY)) : Constants.DEFAULT_FLUSH_TIME;
		this.maxMessageIntoFile = conf.getConfig().get(Constants.MAX_MESSAGES_KEY) != null ? Long.parseLong(conf.getConfig().get(Constants.MAX_MESSAGES_KEY)) : Constants.DEFAULT_MAX_MESSAGES_INTO_FILE;
		this.pathToLocation = conf.getConfig().get(Constants.PATH_TO_LOCATION_KEY) != null ? conf.getConfig().get(Constants.PATH_TO_LOCATION_KEY) : Constants.DEFAULT_PATH_TO_LOCATION;
		this.timeFrameInMinutes = conf.getConfig().get(Constants.TIME_FRAME_KEY) != null ? Integer.parseInt(conf.getConfig().get(Constants.TIME_FRAME_KEY)) : Constants.DEFAULT_TIME_FRAME;
		
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

	public LinkedBlockingQueue<Object> getBuffer()
	{
		return buffer;
	}

	public List<Object> getFlush_buffer()
	{
		return flush_buffer;
	}

	public int getTimeFrameInMinutes()
	{
		return timeFrameInMinutes;
	}

	public String getPathToLocation()
	{
		return pathToLocation;
	}

	public void putMessageIntoBuffer(T message)
	{
		try
		{
			this.buffer.put(message);
		} catch (InterruptedException e)
		{
		}
		if(this.buffer.size() >= this.maxMessageIntoFile)
		{
			synchronized (this)
			{
				if(this.buffer.size() >= this.maxMessageIntoFile)
				{
					//delete timertaks and launch it now
					this.deleteTimerTask();
					copyMessagesOnFlushArray();
					new HDFSWriter<T>(this).run();
					this.setUpTimerTask();
				}
			}
		}
	}

	public void setUpTimerTask()
	{
		this.flushTimer = new Timer();
		this.flushTimer.scheduleAtFixedRate(new HDFSWriter<T>(this), this.flushingTime, this.flushingTime);
	}

	public void deleteTimerTask()
	{
		this.flushTimer.cancel();
	}

	public List<Object> getMessages()
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
		new HDFSWriter<T>(this).run();
	}
}
