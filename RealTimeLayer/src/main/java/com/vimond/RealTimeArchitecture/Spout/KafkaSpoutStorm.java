package com.vimond.RealTimeArchitecture.Spout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

/*
 * Wrapper class for Kafka-Spout API. Do not use it. Go for the direct class imported into the Kafka.kafkalibrary package
 */
@Deprecated
public class KafkaSpoutStorm extends KafkaSpout
{

	private static final long serialVersionUID = -944527298935214167L;

	private static final Logger LOGGER = LogManager.getLogger(KafkaSpoutStorm.class);
	private static final Marker ACK = MarkerManager.getMarker("PERFORMANCES-ACK");

	public KafkaSpoutStorm(SpoutConfig spoutConf)
	{
		super(spoutConf);
	}
	
	@Override
	public void nextTuple()
	{
		super.nextTuple();
		
	}

	@Override
	public void ack(Object msgId)
	{
		super.ack(msgId);
		LOGGER.debug(ACK, msgId + " acked");
	}

	@Override
	public void fail(Object msgId)
	{
		super.fail(msgId);
		LOGGER.error(msgId + "failed");
	}

}
