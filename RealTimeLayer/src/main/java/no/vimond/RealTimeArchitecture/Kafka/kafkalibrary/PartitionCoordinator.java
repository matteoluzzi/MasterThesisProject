package no.vimond.RealTimeArchitecture.Kafka.kafkalibrary;
/**
 * This class has been copied and modified for monitor purposes. All the right goes to Nathan Marz
 */

import java.util.List;

import storm.kafka.GlobalPartitionId;

public interface PartitionCoordinator {
    List<PartitionManager> getMyManagedPartitions();
    PartitionManager getManager(GlobalPartitionId id);
}