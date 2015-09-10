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
import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkState;


public class StaticCoordinator implements PartitionCoordinator {
    Map<GlobalPartitionId, PartitionManager> _managers = new HashMap<GlobalPartitionId, PartitionManager>();
    List<PartitionManager> _allManagers = new ArrayList();

    public StaticCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig config, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        StaticHosts hosts = (StaticHosts) config.hosts;
        List<GlobalPartitionId> allPartitionIds = new ArrayList();
        
        for(HostPort h: hosts.hosts) {
            for(int i=0; i<hosts.partitionsPerHost; i++) {
                allPartitionIds.add(new GlobalPartitionId(h, i));
            }
        }
        for(int i=taskIndex; i<allPartitionIds.size(); i+=totalTasks) {
            GlobalPartitionId myPartition = allPartitionIds.get(i);
            _managers.put(myPartition, new PartitionManager(connections, topologyInstanceId, state, stormConf, config, myPartition));
            
        }
        
        _allManagers = new ArrayList(_managers.values());
    }
    
    public List<PartitionManager> getMyManagedPartitions() {
        return _allManagers;
    }
    
    public PartitionManager getManager(GlobalPartitionId id) {
        return _managers.get(id);
    }
    
}