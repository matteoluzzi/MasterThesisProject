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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.IMetricsContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.DynamicBrokersReader;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.ZkHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkState;

public class ZkCoordinator implements PartitionCoordinator {
    public static Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);
    
    SpoutConfig _spoutConfig;
    int _taskIndex;
    int _totalTasks;
    String _topologyInstanceId;
    Map<GlobalPartitionId, PartitionManager> _managers = new HashMap();
    List<PartitionManager> _cachedList;
    Long _lastRefreshTime = null;
    int _refreshFreqMs;
    DynamicPartitionConnections _connections;
    DynamicBrokersReader _reader;
    ZkState _state;
    Map _stormConf;
    IMetricsContext _metricsContext;
    
    public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        _spoutConfig = spoutConfig;
        _connections = connections;
        _taskIndex = taskIndex;
        _totalTasks = totalTasks;
        _topologyInstanceId = topologyInstanceId;
        _stormConf = stormConf;
	_state = state;

        ZkHosts brokerConf = (ZkHosts) spoutConfig.hosts;
        _refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
        _reader = new DynamicBrokersReader(stormConf, brokerConf.brokerZkStr, brokerConf.brokerZkPath, spoutConfig.topic);
        
    }
    
    public List<PartitionManager> getMyManagedPartitions() {
        if(_lastRefreshTime==null || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
            refresh();
            _lastRefreshTime = System.currentTimeMillis();
        }
        return _cachedList;
    }
    
    void refresh() {
        try {
            LOG.info("Refreshing partition manager connections");
            Map<String, List> brokerInfo = _reader.getBrokerInfo();
            
            Set<GlobalPartitionId> mine = new HashSet();
            for(String host: brokerInfo.keySet()) {
                List info = brokerInfo.get(host);
                long port = (Long) info.get(0);
                HostPort hp = new HostPort(host, (int) port);
                long numPartitions = (Long) info.get(1);
                for(int i=0; i<numPartitions; i++) {
                    GlobalPartitionId id = new GlobalPartitionId(hp, i);
                    if(myOwnership(id)) {
                        mine.add(id);
                    }
                }
            }
            
            Set<GlobalPartitionId> curr = _managers.keySet();
            Set<GlobalPartitionId> newPartitions = new HashSet<GlobalPartitionId>(mine);
            newPartitions.removeAll(curr);
            
            Set<GlobalPartitionId> deletedPartitions = new HashSet<GlobalPartitionId>(curr);
            deletedPartitions.removeAll(mine);
            
            LOG.info("Deleted partition managers: " + deletedPartitions.toString());
            
            for(GlobalPartitionId id: deletedPartitions) {
                PartitionManager man = _managers.remove(id);
                man.close();
            }
            LOG.info("New partition managers: " + newPartitions.toString());
            
            for(GlobalPartitionId id: newPartitions) {
                PartitionManager man = new PartitionManager(_connections, _topologyInstanceId, _state, _stormConf, _spoutConfig, id);
                _managers.put(id, man);
            }
            
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        LOG.info("Finished refreshing");
    }

    public PartitionManager getManager(GlobalPartitionId id) {
        return _managers.get(id);        
    }
    
    private boolean myOwnership(GlobalPartitionId id) {
        int val = Math.abs(id.host.hashCode() + 23 * id.partition);        
        return val % _totalTasks == _taskIndex;
    } 
}