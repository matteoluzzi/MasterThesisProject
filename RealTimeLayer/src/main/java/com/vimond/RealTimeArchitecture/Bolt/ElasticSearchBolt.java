/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.vimond.RealTimeArchitecture.Bolt;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_BATCH_FLUSH_MANUAL;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_BATCH_SIZE_ENTRIES;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE;
import static org.elasticsearch.storm.cfg.StormConfigurationOptions.ES_STORM_BOLT_ACK;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.PartitionWriter;
import org.elasticsearch.storm.TupleUtils;
import org.elasticsearch.storm.cfg.StormSettings;
import org.elasticsearch.storm.serialization.StormTupleBytesConverter;
import org.elasticsearch.storm.serialization.StormTupleFieldExtractor;
import org.elasticsearch.storm.serialization.StormValueWriter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;
import com.vimond.utils.data.StormEvent;

/**
 * Implementation of bolt which writes record on an elasticsearch cluster.
 * Implementation provided by elasticsearch library.</b> Source code has been
 * reported for metrics measurement.
 * 
 * @author matteoremoluzzi
 *
 */

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ElasticSearchBolt implements IRichBolt
{

	private static final long serialVersionUID = -3277652166051865760L;

	private transient static Log log = LogFactory.getLog(ElasticSearchBolt.class);

	private transient Histogram globalLatency;
	private transient Meter counter;

	private Map boltConfig = new LinkedHashMap();

	private transient PartitionWriter writer;
	private transient boolean flushOnTickTuple = true;
	private transient boolean ackWrites = false;

	private transient List<Tuple> inflightTuples = null;
	private transient int numberOfEntries = 0;
	private transient OutputCollector collector;

	private long reportFrequency;
	private String reportPath;

	public ElasticSearchBolt(String target)
	{
		boltConfig.put(ES_RESOURCE_WRITE, target);
	}

	public ElasticSearchBolt(String target, boolean writeAck)
	{
		boltConfig.put(ES_RESOURCE_WRITE, target);
		boltConfig.put(ES_STORM_BOLT_ACK, Boolean.toString(writeAck));
	}

	public ElasticSearchBolt(String target, Map configuration)
	{
		boltConfig.putAll(configuration);
		boltConfig.put(ES_RESOURCE_WRITE, target);
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;

		LinkedHashMap copy = new LinkedHashMap(conf);
		copy.putAll(boltConfig);

		StormSettings settings = new StormSettings(copy);
		flushOnTickTuple = settings.getStormTickTupleFlush();
		ackWrites = settings.getStormBoltAck();

		// trigger manual flush
		if (ackWrites)
		{
			settings.setProperty(ES_BATCH_FLUSH_MANUAL, Boolean.TRUE.toString());

			// align Bolt / es-hadoop batch settings
			numberOfEntries = settings.getStormBulkSize();
			settings.setProperty(ES_BATCH_SIZE_ENTRIES, String.valueOf(numberOfEntries));

			inflightTuples = new ArrayList<Tuple>(numberOfEntries + 1);
		}

		int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();

		InitializationUtils.setValueWriterIfNotSet(settings, StormValueWriter.class, log);
		InitializationUtils.setBytesConverterIfNeeded(settings, StormTupleBytesConverter.class, log);
		InitializationUtils.setFieldExtractorIfNotSet(settings, StormTupleFieldExtractor.class, log);

		writer = RestService.createWriter(settings, context.getThisTaskIndex(), totalTasks, log);

		this.reportFrequency = (Long) conf.get("metric.report.interval");
		this.reportPath = (String) conf.get("metric.report.path");
		initializeMetricsReport();

	}

	public void initializeMetricsReport()
	{
		final MetricRegistry metricRegister = new MetricRegistry();

		this.globalLatency = new Histogram(new UniformReservoir());
		metricRegister.register(MetricRegistry.name(ElasticSearchBolt.class, Thread.currentThread().getName() + "global_latency"), this.globalLatency);

		// register the meter metric
		this.counter = metricRegister.meter(MetricRegistry.name(ElasticSearchBolt.class, Thread.currentThread().getName() + "-events_sec"));

		final CsvReporter reporter = CsvReporter.forRegistry(metricRegister).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.NANOSECONDS).build(new File(this.reportPath));
		reporter.start(this.reportFrequency, TimeUnit.SECONDS);
	}

	public void execute(Tuple input)
	{
		if (flushOnTickTuple && TupleUtils.isTickTuple(input))
		{
			flush();
			return;
		}
		
		long initTime = (Long) input.getValues().remove(1);
		StormEvent x = (StormEvent) input.getValue(0);
		this.globalLatency.update(System.nanoTime() - initTime);
		this.counter.mark();
		
		if (ackWrites)
		{
			inflightTuples.add(input);
		}
		
		try
		{
			writer.repository.writeToIndex(input);

			// manual flush in case of ack writes - handle it here.
			if (numberOfEntries > 0 && inflightTuples.size() >= numberOfEntries)
			{
				flush();
			}

			if (!ackWrites)
			{
				collector.ack(input);
			}
		} catch (RuntimeException ex)
		{
			if (!ackWrites)
			{
				collector.fail(input);
			}
			throw ex;
		}

	}

	private void flush()
	{
		if (ackWrites)
		{
			flushWithAck();
		} else
		{
			flushNoAck();
		}
	}

	private void flushWithAck()
	{
		BitSet flush = null;

		try
		{
			flush = writer.repository.tryFlush();
			writer.repository.discard();
		} catch (EsHadoopException ex)
		{
			// fail all recorded tuples
			for (Tuple input : inflightTuples)
			{
				collector.fail(input);
			}
			inflightTuples.clear();
			throw ex;
		}

		for (int index = 0; index < inflightTuples.size(); index++)
		{
			Tuple tuple = inflightTuples.get(index);
			// bit set means the entry hasn't been removed and thus wasn't
			// written to ES
			if (flush.get(index))
			{
				collector.fail(tuple);
			} else
			{
				collector.ack(tuple);
			}
		}

		// clear everything in bulk to prevent 'noisy' remove()
		inflightTuples.clear();
	}

	private void flushNoAck()
	{
		writer.repository.flush();
	}

	public void cleanup()
	{
		if (writer != null)
		{
			try
			{
				flush();
			} finally
			{
				writer.close();
				writer = null;
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}