package com.vimond.RealTimeArchitecture.Kafka.kafkalibrary;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This class has been copied and modified for monitor purposes. 
 * The code was originally copied from https://github.com/nathanmarz/storm-contrib/tree/master/storm-kafka/src/jvm/storm/kafka
 */

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import kafka.message.Message;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.kafka.KafkaConfig;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkState;
import storm.kafka.trident.KafkaUtils;
import backtype.storm.Config;
import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.vimond.RealTimeArchitecture.Kafka.kafkalibrary.PartitionManager.KafkaMessageId;

// TODO: need to add blacklisting
// TODO: need to make a best effort to not re-emit messages if don't have to
public class KafkaSpout extends BaseRichSpout
{
	private static final long serialVersionUID = -2559222685045548045L;

	public static class MessageAndRealOffset
	{
		public Message msg;
		public long offset;

		public MessageAndRealOffset(Message msg, long offset)
		{
			this.msg = msg;
			this.offset = offset;
		}
	}

	static enum EmitState
	{
		EMITTED_MORE_LEFT, EMITTED_END, NO_EMITTED
	}

	public static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

	String _uuid = UUID.randomUUID().toString();
	SpoutConfig _spoutConfig;
	SpoutOutputCollector _collector;
	PartitionCoordinator _coordinator;
	DynamicPartitionConnections _connections;
	ZkState _state;
	
	private transient MetricRegistry metricRegistry;
	private long reportFrequency;
	private String reportPath;
    
	private transient Meter counter;

	long _lastUpdateMs = 0;

	int _currPartitionIndex = 0;

	public KafkaSpout(SpoutConfig spoutConf)
	{
		_spoutConfig = spoutConf;
	}

	public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector)
	{
		_collector = collector;

		Map stateConf = new HashMap(conf);
		List<String> zkServers = _spoutConfig.zkServers;
		if (zkServers == null)
			zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
		Integer zkPort = _spoutConfig.zkPort;
		if (zkPort == null)
			zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);
		_state = new ZkState(stateConf);

		_connections = new DynamicPartitionConnections(_spoutConfig);

		// using TransactionalState like this is a hack
		int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
		if (_spoutConfig.hosts instanceof KafkaConfig.StaticHosts)
		{
			_coordinator = new StaticCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
		} else
		{
			_coordinator = new ZkCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
		}

		context.registerMetric("kafkaOffset", new IMetric()
		{
			KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_spoutConfig.topic, _connections);

			public Object getValueAndReset()
			{
				List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
				Set<GlobalPartitionId> latestPartitions = new HashSet();
				for (PartitionManager pm : pms)
				{
					latestPartitions.add(pm.getPartition());
				}
				_kafkaOffsetMetric.refreshPartitions(latestPartitions);
				for (PartitionManager pm : pms)
				{
					_kafkaOffsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCompletedOffset());
				}
				return _kafkaOffsetMetric.getValueAndReset();
			}
		}, 60);
		
		context.registerMetric("kafkaPartition", new IMetric()
		{
			public Object getValueAndReset()
			{
				List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
				Map concatMetricsDataMaps = new HashMap();
				for (PartitionManager pm : pms)
				{
					concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
				}
				return concatMetricsDataMaps;
			}
		}, 60);
		
		this.metricRegistry = new MetricRegistry();
		this.reportFrequency = (Long) conf.get("metric.report.interval");
		this.reportPath = (String) conf.get("metric.report.path");
        initializeMetricsReport();
    }
    
    public void initializeMetricsReport()
	{
		final MetricRegistry metricRegister = new MetricRegistry();	
		
		//register the meter metric
		this.counter = metricRegister.meter(MetricRegistry.name(KafkaSpout.class, Thread.currentThread().getName() + "events_sec"));
		
		final CsvReporter reporter = CsvReporter.forRegistry(metricRegister)
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.NANOSECONDS)
				.build(new File(this.reportPath));
		reporter.start(this.reportFrequency, TimeUnit.SECONDS);
	}
	

	@Override
	public void close()
	{
		_state.close();
	}

	public void nextTuple()
	{
		List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
		for (int i = 0; i < managers.size(); i++)
		{

			// in case the number of managers decreased
			_currPartitionIndex = _currPartitionIndex % managers.size();
			EmitState state = managers.get(_currPartitionIndex).next(_collector);
			if (state != EmitState.EMITTED_MORE_LEFT)
			{
				_currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
			}
			if (state != EmitState.NO_EMITTED)
			{
				break;
			}
			
		}

		long now = System.currentTimeMillis();
		if ((now - _lastUpdateMs) > _spoutConfig.stateUpdateIntervalMs)
		{
			commit();
		}
	}

	@Override
	public void ack(Object msgId)
	{
		counter.mark();
		KafkaMessageId id = (KafkaMessageId) msgId;
		PartitionManager m = _coordinator.getManager(id.partition);
		if (m != null)
		{
			m.ack(id.offset);
		}
		// LOGGER.debug(ACK, msgId + " acked");
	}

	@Override
	public void fail(Object msgId)
	{
		KafkaMessageId id = (KafkaMessageId) msgId;
		PartitionManager m = _coordinator.getManager(id.partition);
		if (m != null)
		{
			m.fail(id.offset);
		}
		// LOGGER.debug(FAIL, msgId + "failed");
	}

	@Override
	public void deactivate()
	{
		commit();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(_spoutConfig.scheme.getOutputFields());
	}

	private void commit()
	{
		_lastUpdateMs = System.currentTimeMillis();
		for (PartitionManager manager : _coordinator.getMyManagedPartitions())
		{
			manager.commit();
		}
	}

	public static void main(String[] args)
	{
		// TopologyBuilder builder = new TopologyBuilder();
		// List<String> hosts = new ArrayList<String>();
		// hosts.add("localhost");
		// SpoutConfig spoutConf = SpoutConfig.fromHostStrings(hosts, 8,
		// "clicks", "/kafkastorm", "id");
		// spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		// spoutConf.forceStartOffsetTime(-2);
		//
		// // spoutConf.zkServers = new ArrayList<String>() {{
		// // add("localhost");
		// // }};
		// // spoutConf.zkPort = 2181;
		//
		// builder.setSpout("spout", new KafkaSpout(spoutConf), 3);
		//
		// Config conf = new Config();
		// //conf.setDebug(true);
		//
		// LocalCluster cluster = new LocalCluster();
		// cluster.submitTopology("kafka-test", conf, builder.createTopology());
		//
		// Utils.sleep(600000);
	}
}