package no.vimond.StorageArchitecture.Consumer;

import java.util.Queue;
import java.util.Timer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import no.vimond.StorageArchitecture.Processor.FileSystemMessageProcessor;
import no.vimond.StorageArchitecture.Utils.Constants;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerService;

public class EventsKafkaConsumer extends KafkaConsumerService<VimondEventAny>
{
	private ConcurrentLinkedQueue<VimondEventAny> buffer;
	private Timer flushTimer;
	private AtomicInteger bufferCounter;
	private AtomicBoolean isTaskSet;
	
	public EventsKafkaConsumer(MetricRegistry metricRegistry,
			HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig,
			KafkaConsumerConfig consumerConfig, FileSystemMessageProcessor fsProcessor)
	{
		super(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, fsProcessor);
		//bind message processor to this
		fsProcessor.setEventsKafkaConsumer(this);
		this.buffer = new ConcurrentLinkedQueue<VimondEventAny>();
		
		this.bufferCounter = new AtomicInteger(0);
		this.isTaskSet = new AtomicBoolean(false);
		this.setUpTimerTask(Constants.DEFAULT_FLUSH_TIME);
		
	}

	public void putMessageIntoBuffer(VimondEventAny message)
	{
		this.buffer.add(message);
		this.bufferCounter.incrementAndGet();
		if(this.getTaskStatus().get() == false)
			this.setUpTimerTask(Constants.DEFAULT_FLUSH_TIME);
		if(this.bufferCounter.get() >= Constants.MAX_MESSAGES_INTO_FILE)
		{
			this.deleteTimerTask();
			this.executeTaskNow();
			this.setTaskStatus(false);
			this.bufferCounter.set(0);
		}
	}
	
	public void setUpTimerTask(long delay)
	{
		this.flushTimer = new Timer();
		this.flushTimer.schedule(new FileSystemWriter(this), delay * 60 * 1000);
		this.setTaskStatus(true);
	}
	
	public void deleteTimerTask()
	{
		this.flushTimer.cancel();
	}
	
	public void executeTaskNow()
	{
		new FileSystemWriter(this).run();
		this.buffer.clear();
	}
	
	public Queue getMessages()
	{
		return buffer;
	}
	
	public void setTaskStatus(boolean running)
	{
		synchronized (this.isTaskSet)
		{
			this.isTaskSet.set(running);
		}	
	}
	
	public AtomicBoolean getTaskStatus()
	{
		synchronized (this.isTaskSet)
		{
			return this.isTaskSet;
		}
	}
}
