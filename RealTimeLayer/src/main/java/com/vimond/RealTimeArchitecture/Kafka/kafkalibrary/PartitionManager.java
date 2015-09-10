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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;
import kafka.common.OffsetOutOfRangeException;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkState;
import storm.kafka.trident.MaxMetric;
import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.ecyrd.speed4j.StopWatch;
import com.google.common.collect.ImmutableMap;
import com.vimond.RealTimeArchitecture.Bolt.RouterBolt;
import com.vimond.RealTimeArchitecture.Kafka.kafkalibrary.KafkaSpout.EmitState;
import com.vimond.utils.data.Constants;

public class PartitionManager {
    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);
	private int emittedTuple;
    
    private final CombinedMetric _fetchAPILatencyMax;
    private final ReducedMetric _fetchAPILatencyMean;
    private final CountMetric _fetchAPICallCount;
    private final CountMetric _fetchAPIMessageCount;
    
	private long reportFrequency;
	private String reportPath;
    
	private transient Meter counter;

    static class KafkaMessageId {
        public GlobalPartitionId partition;
        public long offset;

        public KafkaMessageId(GlobalPartitionId partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    Long _emittedToOffset;
    SortedSet<Long> _pending = new TreeSet<Long>();
    Long _committedTo;
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    GlobalPartitionId _partition;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    ZkState _state;
    Map _stormConf;


    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, ZkState state, Map stormConf, SpoutConfig spoutConfig, GlobalPartitionId id) {
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.partition);
	_state = state;
        _stormConf = stormConf;

        String jsonTopologyId = null;
        Long jsonOffset = null;
        try {
            Map<Object, Object> json = _state.readJSON(committedPath());
            if(json != null) {
                jsonTopologyId = (String)((Map<Object,Object>)json.get("topology")).get("id");
                jsonOffset = (Long)json.get("offset");
            }
        }
        catch(Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + committedPath(), e);
        }

        if(!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
            _committedTo = _consumer.getOffsetsBefore(spoutConfig.topic, id.partition, spoutConfig.startOffsetTime, 1)[0];
	    LOG.info("Using startOffsetTime to choose last commit offset.");
        } else if(jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
            _committedTo = _consumer.getOffsetsBefore(spoutConfig.topic, id.partition, -1, 1)[0];
	    LOG.info("Setting last commit offset to HEAD.");
        } else {
            _committedTo = jsonOffset;
	    LOG.info("Read last commit offset from zookeeper: " + _committedTo);
        }

        LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
        _emittedToOffset = _committedTo;

        _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
        _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        _fetchAPICallCount = new CountMetric();
        _fetchAPIMessageCount = new CountMetric();
        this.reportFrequency = (Long) stormConf.get("metric.report.interval");
		this.reportPath = (String) stormConf.get("metric.report.path");
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

    public Map getMetricsDataMap() {
        Map ret = new HashMap();
        ret.put(_partition + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
        ret.put(_partition + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
        ret.put(_partition + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
        ret.put(_partition + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
        return ret;
    }

    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector) {
    	  try
          {
        if(_waitingToEmit.isEmpty()) fill();
        while(true) {
            MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
            if(toEmit==null) {
                return EmitState.NO_EMITTED;
            }
            Iterable<List<Object>> tups = _spoutConfig.scheme.deserialize(Utils.toByteArray(toEmit.msg.payload()));
            if(tups!=null) {
                for(List<Object> tup: tups)
                {
                    collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
                    this.counter.mark();
                }
                break;
            } else {
                ack(toEmit.offset);
            }
        }
        if(!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
          }
        catch(Exception e)
        {
        	return EmitState.NO_EMITTED;
        }
    }

    private void fill() throws Exception {
        //LOG.info("Fetching from Kafka: " + _consumer.host() + ":" + _partition.partition + " from offset " + _emittedToOffset);
        long start = System.nanoTime();
      
        	 ByteBufferMessageSet msgs = _consumer.fetch(
                     new FetchRequest(
                         _spoutConfig.topic,
                         _partition.partition,
                         _emittedToOffset,
                         _spoutConfig.fetchSizeBytes));
             try{
             	_fetchAPIMessageCount.incrBy(msgs.underlying().size());
             }
             catch(OffsetOutOfRangeException e)
             {
             	//Request the last offset available for the current partition of the current topic.
             	//TODO change retention policy in kafka broker
             	LOG.warn("Error in fetching the message according to the stored offset. Going to use the latest available");
             	_emittedToOffset = this.queryPartitionOffsetLatestTime();
             	msgs = _consumer.fetch(
                         new FetchRequest(
                             _spoutConfig.topic,
                             _partition.partition,
                             _emittedToOffset,
                             _spoutConfig.fetchSizeBytes));
             }
             
             long end = System.nanoTime();
             long millis = (end - start) / 1000000;
             _fetchAPILatencyMax.update(millis);
             _fetchAPILatencyMean.update(millis);
             _fetchAPICallCount.incr();

             int numMessages = msgs.underlying().size();
             if(numMessages>0) {
               LOG.debug("Fetched " + numMessages + " messages from Kafka: " + _consumer.host() + ":" + _partition.partition);
             }
             for(MessageAndOffset msg: msgs) {
                 _pending.add(_emittedToOffset);
                 _waitingToEmit.add(new MessageAndRealOffset(msg.message(), _emittedToOffset));
                 _emittedToOffset = msg.offset();
             }
             if(numMessages>0) {
               LOG.debug("Added " + numMessages + " messages from Kafka: " + _consumer.host() + ":" + _partition.partition + " to internal buffers");
             }
        
       
    }

    public void ack(Long offset) {
        _pending.remove(offset);
    }

    public void fail(Long offset){
        //TODO: should it use in-memory ack set to skip anything that's been acked but not committed???
        // things might get crazy with lots of timeouts
        if(_emittedToOffset > offset) {
            _emittedToOffset = offset;
            _pending.tailSet(offset).clear();
        }
    }

    public void commit() {
        LOG.debug("Committing offset for " + _partition);
        long committedTo;
        if(_pending.isEmpty()) {
            committedTo = _emittedToOffset;
        } else {
            committedTo = _pending.first();
        }
        if(committedTo!=_committedTo) {
            LOG.debug("Writing committed offset to ZK: " + committedTo);

            Map<Object, Object> data = (Map<Object,Object>)ImmutableMap.builder()
                .put("topology", ImmutableMap.of("id", _topologyInstanceId,
                                                 "name", _stormConf.get(Config.TOPOLOGY_NAME)))
                .put("offset", committedTo)
                .put("partition", _partition.partition)
                .put("broker", ImmutableMap.of("host", _partition.host.host,
                                               "port", _partition.host.port))
                .put("topic", _spoutConfig.topic).build();
	    _state.writeJSON(committedPath(), data);

            LOG.debug("Wrote committed offset to ZK: " + committedTo);
            _committedTo = committedTo;
        }
        LOG.debug("Committed offset " + committedTo + " for " + _partition);
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition;
    }

    public long queryPartitionOffsetLatestTime() {
        return _consumer.getOffsetsBefore(_spoutConfig.topic, _partition.partition,
                                          OffsetRequest.LatestTime(), 1)[0];
    }

    public long lastCommittedOffset() {
        return _committedTo;
    }

    public long lastCompletedOffset() {
        if(_pending.isEmpty()) {
            return _emittedToOffset;
        } else {
            return _pending.first();
        }
    }

    public GlobalPartitionId getPartition() {
        return _partition;
    }

    public void close() {
        _connections.unregister(_partition.host, _partition.partition);
    }
}
