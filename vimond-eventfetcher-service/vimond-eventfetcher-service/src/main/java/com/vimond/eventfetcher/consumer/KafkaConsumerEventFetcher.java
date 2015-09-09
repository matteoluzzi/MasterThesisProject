package com.vimond.eventfetcher.consumer;

import java.util.List;

import com.vimond.common.messages.MessageConsumer;

/**
 * Interface for grouping the KafkaConsumers
 * @author matteoremoluzzi
 *
 * @param <T>
 */
public interface KafkaConsumerEventFetcher<T> extends MessageConsumer<T>
{
	public void putMessageIntoBuffer(T message);

	public String getHDFSPathToLocation();
	
	public List<Object> getMessages();

	public int getTimeFrameInMinutes();
}
