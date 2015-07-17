package no.vimond.StorageArchitecture.Consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingQueue;

import no.vimond.StorageArchitecture.Processor.BatchProcessor;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Writer.HDFSWriter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerService;
import com.vimond.common.kafka07.consumer.MessageProcessor;

/**
 * put and drainTo methods are thread-safe. Usage of internal locking mechanism
 * @author matteoremoluzzi
 *
 */
public class EventsKafkaConsumer extends KafkaConsumerService<String>
{
	private LinkedBlockingQueue<Object> buffer;
	private List<Object> flush_buffer;
	private Timer flushTimer;
	
	private long flushingTime;
	private long maxMessageIntoFile;
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public EventsKafkaConsumer(MetricRegistry metricRegistry,
			HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig,
			KafkaConsumerConfig consumerConfig, BatchProcessor fsProcessor, Properties appProps)
	{
		super(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, (MessageProcessor) fsProcessor);
		//bind message processor to this
		fsProcessor.setEventsKafkaConsumer(this);
		this.buffer = new LinkedBlockingQueue<Object>();
		this.flush_buffer = new ArrayList<Object>();
		
		this.flushingTime = Long.parseLong((String) appProps.getOrDefault(Constants.FLUSHING_TIME_KEY, Constants.DEFAULT_FLUSH_TIME));
		this.maxMessageIntoFile = Long.parseLong((String) appProps.getOrDefault(Constants.MAX_MESSAGES_KEY, Constants.DEFAULT_MAX_MESSAGES_INTO_FILE));
		
		this.setUpTimerTask(this.flushingTime);
	}

	public void putMessageIntoBuffer(Object message) throws InterruptedException
	{
		this.buffer.put(message);
		if(this.buffer.size() >= this.maxMessageIntoFile)
		{
			synchronized (this)
			{
				if(this.buffer.size() >= this.maxMessageIntoFile)
				{
					//delete timertaks and launch it now
					this.deleteTimerTask();
					copyMessagesOnFlushArray();
					new HDFSWriter(this).run();
					this.setUpTimerTask(this.flushingTime);
				}
			}
		}
	}
	
	public void setUpTimerTask(long delay)
	{
		this.flushTimer = new Timer();
		this.flushTimer.scheduleAtFixedRate(new HDFSWriter(this), delay * 60 * 1000, delay * 60 * 1000);
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
		if(this.buffer.size() > 0)
			this.buffer.drainTo(flush_buffer);
	}
	
	public void shutdown()
	{
		this.executor.shutdown();
		//flush the buffer to disk
		new HDFSWriter(this).run();
	}
	
	
}
