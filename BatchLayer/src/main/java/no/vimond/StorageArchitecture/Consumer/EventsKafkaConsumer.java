package no.vimond.StorageArchitecture.Consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingQueue;

import no.vimond.StorageArchitecture.Processor.FileSystemMessageProcessor;
import no.vimond.StorageArchitecture.Utils.Constants;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerService;

/**
 * put and drainTo methods are thread-safe. Using of internal locking mechanism
 * @author matteoremoluzzi
 *
 */
public class EventsKafkaConsumer extends KafkaConsumerService<VimondEventAny>
{
	private LinkedBlockingQueue<VimondEventAny> buffer;
	private List<VimondEventAny> flush_buffer;
	private Timer flushTimer;
	
	public EventsKafkaConsumer(MetricRegistry metricRegistry,
			HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig,
			KafkaConsumerConfig consumerConfig, FileSystemMessageProcessor fsProcessor)
	{
		super(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, fsProcessor);
		//bind message processor to this
		fsProcessor.setEventsKafkaConsumer(this);
		this.buffer = new LinkedBlockingQueue<VimondEventAny>();
		this.flush_buffer = new ArrayList<VimondEventAny>();
		this.setUpTimerTask(Constants.DEFAULT_FLUSH_TIME);
	}

	public void putMessageIntoBuffer(VimondEventAny message) throws InterruptedException
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
	
	public List<VimondEventAny> getMessages()
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
