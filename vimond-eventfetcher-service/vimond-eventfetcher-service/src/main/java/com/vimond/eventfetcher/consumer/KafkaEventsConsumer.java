package com.vimond.eventfetcher.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.ecyrd.speed4j.StopWatch;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.common.messages.MessageConsumer;
import com.vimond.common.zkhealth.ZookeeperHealthCheck;
import com.vimond.eventfetcher.configuration.ProcessorConfiguration;
import com.vimond.pailStructure.TimeFramePailStructure;
import com.vimond.eventfetcher.processor.BatchProcessor;
import com.vimond.eventfetcher.util.Constants;

/**
 * Reliable version of a KafkaConsumer. It processes the events in batch commits
 * the offset only after processing them<b> Usage of CyclicBarrier for thread
 * synchronization
 * 
 * @author matteoremoluzzi
 *
 * @param <T>
 */

public class KafkaEventsConsumer<T> implements MessageConsumer<T>, KafkaConsumerEventFetcher<T>
{
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final KafkaConfig kafkaConfig;
	protected final KafkaConsumerConfig consumerConfig;
	protected MessageProcessor<T> messageProcessor;
	protected ExecutorService executor;
	protected List<KafkaStream<T>> streams;
	protected ConsumerConnector consumerConnector;
	protected final AtomicBoolean isRunning = new AtomicBoolean(false);
	protected final String name;
	protected long batchSize;
	protected long flushingTime;
	protected long millsToSleepWhenError = 1000 * 5;
	protected LinkedBlockingQueue<T> buffer = new LinkedBlockingQueue<T>();
	protected long batchEndTime;

	protected String HDFSPathToLocation;
	private int timeFrameInMinutes;

	protected final MetricRegistry metricRegistry;
	protected ZookeeperHealthCheck healthCheck;
	
	@SuppressWarnings("unchecked")
	public KafkaEventsConsumer(MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig, KafkaConsumerConfig consumerConfig, BatchProcessor fsProcessor, ProcessorConfiguration conf)
	{
		fsProcessor.setEventsKafkaConsumer(this);
		this.metricRegistry = metricRegistry;
		this.kafkaConfig = kafkaConfig;
		this.consumerConfig = consumerConfig;
		this.messageProcessor = (MessageProcessor<T>) fsProcessor;
		this.name = "KafkaConsumer[" + consumerConfig.topic + "][" + consumerConfig.groupid + "]";
		// this.consumerConfig.properties.put("consumer.timeout.ms", "30");
		this.flushingTime = conf.getConfig().get(Constants.FLUSHING_TIME_KEY) != null ? Long.parseLong(conf.getConfig().get(Constants.FLUSHING_TIME_KEY)) : Constants.DEFAULT_FLUSH_TIME;
		this.batchSize = conf.getConfig().get(Constants.MAX_MESSAGES_KEY) != null ? Long.parseLong(conf.getConfig().get(Constants.MAX_MESSAGES_KEY)) : Constants.DEFAULT_MAX_MESSAGES_INTO_FILE;
		this.HDFSPathToLocation = conf.getConfig().get(Constants.HDFS_PATH_TO_LOCATION_KEY) != null ? conf.getConfig().get(Constants.HDFS_PATH_TO_LOCATION_KEY) : Constants.DEFAULT_HDFS_PATH_TO_LOCATION;
		this.timeFrameInMinutes = conf.getConfig().get(Constants.TIME_FRAME_KEY) != null ? Integer.parseInt(conf.getConfig().get(Constants.TIME_FRAME_KEY)) : Constants.DEFAULT_TIME_FRAME;

		this.initializeTimeFramePail();
		
		setupHealthchecks(healthCheckRegistry, kafkaConfig);

		// procedure for controlled shutdown
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			public void run()
			{
				try
				{
					shutdown();
				} catch (Exception e)
				{
					flushToHdfs();
				}
			}
		});
	}

	public KafkaEventsConsumer(MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig, KafkaConsumerConfig consumerConfig, MessageProcessor<T> messageProcessor)
	{
		this.metricRegistry = metricRegistry;
		this.kafkaConfig = kafkaConfig;
		this.consumerConfig = consumerConfig;
		this.messageProcessor = messageProcessor;
		this.name = "KafkaConsumer[" + consumerConfig.topic + "][" + consumerConfig.groupid + "]";
		this.batchSize = Constants.DEFAULT_MAX_MESSAGES_INTO_FILE;
		this.flushingTime = Constants.DEFAULT_FLUSH_TIME;
		this.HDFSPathToLocation = Constants.DEFAULT_HDFS_PATH_TO_LOCATION;
		this.timeFrameInMinutes = Constants.DEFAULT_TIME_FRAME;
		setupHealthchecks(healthCheckRegistry, kafkaConfig);
		
		this.initializeTimeFramePail();

		// procedure for controlled shutdown
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			public void run()
			{
				try
				{
					shutdown();
				} catch (Exception e)
				{
					flushToHdfs();
				}
			}
		});
	}

	private void setupHealthchecks(HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig)
	{
		healthCheck = new ZookeeperHealthCheck(kafkaConfig.hosts);
		healthCheckRegistry.register("zk_kafka_consumer_0.7", healthCheck);
	}

	/**
	 * The consumer name, used in thread name generation
	 * 
	 * @return the name of topic and group. KafkaConsumer['topic']['groupid']
	 */
	public String getName()
	{
		return name;
	}

	public int getTimeFrameInMinutes()
	{
		return timeFrameInMinutes;
	}

	public String getHDFSPathToLocation()
	{
		return HDFSPathToLocation;
	}

	@Override
	public List<Object> getMessages()
	{
		return new ArrayList<Object>(this.buffer);
	}

	private void setBatchEndTime(long endTime)
	{
		this.batchEndTime = endTime;
	}

	private long getBatchEndTime()
	{
		return this.batchEndTime;
	}

	// Returns an AtomicInteger that can be checks on in the future to check if
	// this consumer is running
	public AtomicBoolean getIsRunning()
	{
		return isRunning;
	}

	public long getBufferSize()
	{
		return this.buffer.size();
	}

	public void clearBuffer()
	{
		this.buffer.clear();
	}

	public void commitOffsets()
	{
		consumerConnector.commitOffsets();
	}

	@Override
	public void start() throws IllegalStateException
	{
		if (isRunning.get())
		{
			throw new IllegalStateException("Has already been started: " + name);
		}
		try
		{
			healthCheck.start();
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("Starting " + consumerConfig.consumerThreads + " threads for " + name);

		// Build config from config-fields, with fill-ins from config.properties
		// First use from kafkaConfig..
		Properties p = new Properties();
		for (String key : kafkaConfig.properties.keySet())
		{
			p.setProperty(key, kafkaConfig.properties.get(key));
		}
		// ...then use/override from consumer specific configs
		for (String key : consumerConfig.properties.keySet())
		{
			p.setProperty(key, consumerConfig.properties.get(key));
		}

		// now override from fields
		p.setProperty("zk.connect", kafkaConfig.hosts);

		p.setProperty("groupid", consumerConfig.groupid);
		// p.setProperty("consumer.timeout.ms", "500");

		// Create the connection to the cluster
		ConsumerConfig consumerConfig = new ConsumerConfig(p);
		consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

		if (messageProcessor == null)
		{
			throw new RuntimeException("Cannot start - messageProcessor is null");
		}

		final Decoder<T> decoder = messageProcessor.getDecoderSingleton();

		// create X partitions of the stream for topic “test”, to allow X
		// threads to consume
		Map<String, List<KafkaStream<T>>> topicMessageStreams = consumerConnector.createMessageStreams(ImmutableMap.of(this.consumerConfig.topic, this.consumerConfig.consumerThreads), decoder);

		logger.info("Create message streams");

		streams = topicMessageStreams.get(this.consumerConfig.topic);

		// create list of X threads to consume from each of the partitions
		executor = getExecutorService();
		
		CyclicBarrier barrier = new CyclicBarrier(this.consumerConfig.consumerThreads, new FlushAndCommit(consumerConnector, logger, this));

		// consume the messages in the threads
		OurMetrics metrics = new OurMetrics(metricRegistry, name);

		// set the first batch end time, the next ones will be set by the thread
		// in the barrier
		setBatchEndTime(System.currentTimeMillis() + this.flushingTime);

		for (final KafkaStream<T> stream : streams)
		{
			executor.submit(new KafkaStreamReader<T>(metrics, logger, name, millsToSleepWhenError, messageProcessor, stream, this, barrier));
		}
		isRunning.set(true);
	}

	protected ExecutorService getExecutorService()
	{
		return Executors.newFixedThreadPool(this.consumerConfig.consumerThreads, new ThreadFactoryBuilder().setNameFormat(name + "-%d").build());
	}

	protected static class OurMetrics
	{
		public final Counter receivedMessages;
		public final Counter receivedMessagesWithError;
		public final Counter retryCountWhenProcessingEvent;
		public final Timer messageProcessingTime;

		public OurMetrics(MetricRegistry metricsRegistry, String name)
		{
			receivedMessages = metricsRegistry.counter(name + "_kafka-consumer-receivedMessages");
			receivedMessagesWithError = metricsRegistry.counter(name + "_kafka-consumer-receivedMessagesWithError");
			retryCountWhenProcessingEvent = metricsRegistry.counter(name + "_kafka-consumer-retryCountWhenProcessingEvent");
			messageProcessingTime = metricsRegistry.timer(name + "_kafka-consumer-messageProcessingTime");
		}
	}

	/**
	 * Task for committing the offset of the current batch. Executed by the last
	 * thread of the executor service
	 * 
	 * @author matteoremoluzzi
	 *
	 */
	protected class FlushAndCommit implements Runnable
	{

		private final ConsumerConnector connector;
		private final Logger logger;
		private final KafkaEventsConsumer<T> consumer;

		public FlushAndCommit(ConsumerConnector connector, Logger logger, KafkaEventsConsumer<T> consumer)
		{
			this.connector = connector;
			this.logger = logger;
			this.consumer = consumer;
		}

		/**
		 * try to write the batch into hdfs, applying a exponential backoff
		 * retrying mechanism for 10 minutes
		 */
		@Override
		public void run()
		{

			Callable<Boolean> writeFunction = new Callable<Boolean>()
			{

				@Override
				public Boolean call() throws Exception
				{
					return flushToHdfs();
				}
			};

			Retryer<Boolean> retryer = RetryerBuilder.<Boolean> newBuilder().retryIfExceptionOfType(IOException.class).retryIfRuntimeException().retryIfResult(Predicates.<Boolean> equalTo(false)).withWaitStrategy(WaitStrategies.exponentialWait(50, 5, TimeUnit.MINUTES)).withStopStrategy(StopStrategies.stopAfterDelay(10, TimeUnit.MINUTES)).build();

			boolean writeSucceeded = false;
			try
			{
				writeSucceeded = retryer.call(writeFunction);
			} catch (RetryException e)
			{
				logger.info("error while flushing data and committing the offset: {}", e);
				this.consumer.setBatchEndTime(System.currentTimeMillis() + this.consumer.flushingTime);
				return;
			} catch (ExecutionException e)
			{
				logger.info("error while flushing data and committing the offset: {}", e);
				this.consumer.setBatchEndTime(System.currentTimeMillis() + this.consumer.flushingTime);
				return;
			}

			if (writeSucceeded)
			{
				logger.info("Committing the offset");
				this.connector.commitOffsets();
				this.consumer.clearBuffer();
				this.consumer.setBatchEndTime(System.currentTimeMillis() + this.consumer.flushingTime);
			} else
			{
				logger.warn("Error while wrting the offset, Namenode not reachable?");
				this.consumer.setBatchEndTime(System.currentTimeMillis() + this.consumer.flushingTime);
			}
		}
	}

	protected static class KafkaStreamReader<T> implements Runnable
	{

		private final Logger logger;
		private final String name;
		private final long millsToSleepWhenError;
		private final MessageProcessor<T> messageProcessor;
		private final OurMetrics metrics;
		private final KafkaEventsConsumer<T> consumer;
		private ConsumerIterator<T> it;
		private final CyclicBarrier barrier;
		private int processedTuples;
		private final org.apache.logging.log4j.Logger performaces = LogManager.getLogger(KafkaStreamReader.class);
		
		private StopWatch timer;
		private final static Marker ERROR = MarkerManager.getMarker("BATCH-ERRORS");
		private final static Marker THROUGHPUT = MarkerManager.getMarker("PERFORMANCES-BATCH-THROUGHPUT");
		private static final double FROM_NANOS_TO_SECONDS = 0.000000001;

		public KafkaStreamReader(OurMetrics metrics, Logger logger, String name, long millsToSleepWhenError, MessageProcessor<T> messageProcessor, KafkaStream<T> stream, KafkaEventsConsumer<T> consumer, CyclicBarrier barrier)
		{
			this.metrics = metrics;
			this.logger = logger;
			this.name = name;
			this.millsToSleepWhenError = millsToSleepWhenError;
			this.messageProcessor = messageProcessor;
			this.consumer = consumer;
			this.barrier = barrier;
			this.it = stream.iterator();
			this.timer = new StopWatch();
		}

		public void run()
		{
			try
			{
				final String name = Thread.currentThread().getName();
				logger.info("Thread " + name + " starting to process stream");
				long batchEndTime = this.consumer.getBatchEndTime();
				boolean read = true;
				this.timer.start();

				while (read)
				{
					try
					{
						if (it.hasNext())
						{
							MessageAndMetadata<T> msgAndMetadata = it.next();
							metrics.receivedMessages.inc();
							final boolean processed = processMessage(msgAndMetadata.message());
							if (msgAndMetadata.message() == null || !processed)
								metrics.receivedMessagesWithError.inc();
							else
							{
								if (++processedTuples % 10000 == 0)
								 {
									 this.timer.stop();
									 double avg_throughput = 10000 /	 (this.timer.getTimeNanos() * FROM_NANOS_TO_SECONDS);
									 performaces.info(THROUGHPUT, avg_throughput);
									 processedTuples = 0;
									 this.timer.start();
								 }
							}
						}
					} catch (ConsumerTimeoutException e) // caught when there
															// are no messages
															// available in the
															// set timeout
					{
					}
					if (this.consumer.getBufferSize() >= this.consumer.batchSize || System.currentTimeMillis() >= batchEndTime)
					{
						try
						{
							barrier.await();
							// after the barrier all thread are synchronized
							// with the next batch execution
							batchEndTime = this.consumer.getBatchEndTime();
						} catch (Exception e)
						{
							logger.debug("Exception while waiting for batch completion: {}", e.getMessage());
							break;
						}
					}
				}

			} catch (Throwable e)
			{
				// TODO: Revise this, guess
				logger.error("Fatal error", e);
			}
		}

		protected boolean processMessage(T message)
		{
			int retryCount = 0;
			while (true)
			{

				if (retryCount > 0)
				{
					metrics.retryCountWhenProcessingEvent.inc();
					logger.warn("Retrying to process message. retryCount: {}", retryCount);
				}
				Timer.Context timer = metrics.messageProcessingTime.time();
				try
				{
					final boolean result = messageProcessor.process(message, retryCount);
					timer.stop();
					return result;
				} catch (Exception e)
				{
					timer.stop();
					logger.warn("Error processing message from {}: {}", name, message, e);
					// Sleep some time just to prevent us from using all cpu
					Uninterruptibles.sleepUninterruptibly(millsToSleepWhenError, TimeUnit.MILLISECONDS);
					if (Thread.interrupted())
					{
						logger.warn("Interrupted (now cleared), shutting down processing msg from {}: {}", name, message);
						return false;
					}
				}
				retryCount++;
			}
		}
	}

	public void registerHealthChecks(HealthCheckRegistry healthCheckRegistry)
	{
		healthCheckRegistry.register("kafka_executor", new HealthCheck()
		{
			@Override
			protected Result check() throws Exception
			{
				if (!executor.isShutdown() || !executor.isTerminated())
					return Result.healthy(executor.toString());
				else
					return Result.unhealthy(executor.toString());
			}
		});

	}

	public synchronized void stop() throws Exception
	{
		shutdown();
	}

	public synchronized void shutdown() throws Exception
	{
		shutdownConsumer();
		shutdownExecutor();
		flushToHdfs();
		healthCheck.stop();
		isRunning.set(false);
	}

	private void shutdownExecutor() throws Exception
	{
		if (executor == null)
		{
			logger.info("Tried to shutdown non-existing executor");
			return;
		}

		executor.shutdown();
		executor.awaitTermination(60, TimeUnit.SECONDS);
		if (!executor.isTerminated())
		{
			logger.warn("Executor not stopping by it self - forcing shutdown: " + name);
			executor.shutdownNow();
		}
	}

	protected void shutdownConsumer()
	{
		logger.info("Stopping " + name);
		consumerConnector.shutdown();
	}

	public void putMessageIntoBuffer(T message)
	{
		this.buffer.add(message);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean flushToHdfs()
	{
		// Write only if there are messages
		if (buffer.size() > 0)
		{
			logger.debug("Going to write {} messages to HDFS", buffer.size());
			
			Pail pail;
			try
			{
				pail = new Pail(getHDFSPathToLocation());
			} catch (java.lang.IllegalArgumentException | IOException e)
			{
				logger.debug("Error when accessing pail: {}", e.getMessage());
				try
				{
					pail = Pail.create(getHDFSPathToLocation(), new TimeFramePailStructure());

				} catch (IOException e1)
				{
					logger.error("Error when creating pail: {}", e.getMessage());
					return false;
				}
			}
			TypedRecordOutputStream os;
			try
			{
				os = pail.openWrite();
				os.writeObjects(buffer.toArray());
				os.close();
			} catch (IOException e)
			{
				logger.error("Error when writing on HDFS: {}", e.getMessage());
				return false;
			}
			buffer.clear();
			return true;
		}
		return false;
	}
	
	public void initializeTimeFramePail()
	{
		TimeFramePailStructure.initialize(getTimeFrameInMinutes());
	}

}
