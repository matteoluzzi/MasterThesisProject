package no.vimond.StorageArchitecture.Consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingQueue;

import no.vimond.StorageArchitecture.Processor.FileSystemMessageProcessor;
import no.vimond.StorageArchitecture.Processor.HDFSMessageProcessor;
import no.vimond.StorageArchitecture.Utils.Constants;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerService;
import com.vimond.common.kafka07.consumer.MessageProcessor;

/**
 * put and drainTo methods are thread-safe. Using of internal locking mechanism
 * @author matteoremoluzzi
 *
 */
public class EventsKafkaConsumer extends KafkaConsumerService<String>
{
	private LinkedBlockingQueue<Object> buffer;
	private List<Object> flush_buffer;
	private Timer flushTimer;
	
	public EventsKafkaConsumer(MetricRegistry metricRegistry,
			HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig,
			KafkaConsumerConfig consumerConfig, MessageProcessor fsProcessor)
	{
		super(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, fsProcessor);
		//bind message processor to this
		((HDFSMessageProcessor) fsProcessor).setEventsKafkaConsumer(this);
		this.buffer = new LinkedBlockingQueue<Object>();
		this.flush_buffer = new ArrayList<Object>();
		this.setUpTimerTask(Constants.DEFAULT_FLUSH_TIME);
	}

	public void putMessageIntoBuffer(Object message) throws InterruptedException
	{
		this.buffer.put(message);
		if(this.buffer.size() >= Constants.MAX_MESSAGES_INTO_FILE)
		{
			synchronized (this)
			{
				if(this.buffer.size() >= Constants.MAX_MESSAGES_INTO_FILE)
				{
					//delete timertaks and launch it now
					this.deleteTimerTask();
					copyMessagesOnFlushArray();
					new FileSystemWriter(this).run();
					this.setUpTimerTask(Constants.DEFAULT_FLUSH_TIME);
				}
			}
		}
	}
	
	public void setUpTimerTask(long delay)
	{
		this.flushTimer = new Timer();
		this.flushTimer.scheduleAtFixedRate(new FileSystemWriter(this), delay * 60 * 1000, delay * 60 * 1000);
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
		new FileSystemWriter(this).run();
	}
}
