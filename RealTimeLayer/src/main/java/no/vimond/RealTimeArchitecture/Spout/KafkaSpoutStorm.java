package no.vimond.RealTimeArchitecture.Spout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

public class KafkaSpoutStorm extends KafkaSpout
{

	private static final long serialVersionUID = -944527298935214167L;

	private static final Logger LOGGER = LogManager.getLogger(KafkaSpoutStorm.class);

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
		LOGGER.debug(msgId + " acked");
	}

	@Override
	public void fail(Object msgId)
	{
		super.fail(msgId);
		LOGGER.error(msgId + "failed");
	}

}
