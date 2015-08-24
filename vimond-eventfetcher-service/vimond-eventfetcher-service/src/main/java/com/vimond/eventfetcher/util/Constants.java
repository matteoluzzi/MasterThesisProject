package com.vimond.eventfetcher.util;

public class Constants
{
	public static final String DEFAULT_ZK_LOCATION = "localhost:2181";
	public static final String DEFAULT_TOPIC = "player-events";
	public static final String DEFAULT_CONSUMER_GROUP_RT = "kafka-consumer-group_rt";
	public static final String DEFAULT_CONSUMER_GROUP_B = "kafka-consumer-group_b";
	public static final long DEFAULT_FLUSH_TIME = 1;
	public static final long DEFAULT_MAX_MESSAGES_INTO_FILE = 50000;
	public static final int DEFAULT_CONSUMER_THREAD = 1;
	public static final int DEFAULT_TIME_FRAME = 5;
	public static final String DEFAULT_PATH_TO_LOCATION = "hdfs://localhost:9000/user/matteoremoluzzi/dataset/master";
	
	public static final String PROPERTIES_FILE = "kafkasettings.properties";
	public static final String GROUP_KEY_ID = "group.id";
	
	public static final String IP_STREAM = "ip_stream";
	public static final String EVENT_MESSAGE = "event";
	public static final String IP_MESSAGE = "ip";
	
	public static final String COUNTRY_NAME = "geo.country";
	public static final String LATITUDE = "geo.lat";
	public static final String LONGITUDE = "geo.lon";
	
	public static final String NEW_DATA_PATH = "hdfs://localhost:9000/user/matteoremoluzzi/dataset/master";
	
	public static final String ZK_KEY = "zkServer";
	public static final String CONSUMER_THREAD_KEY = "consumerThread";
	public static final String FLUSHING_TIME_KEY = "flushingTime";
	public static final String CONSUMER_GROUP_KEY = "consumerId_b";
	public static final String MAX_MESSAGES_KEY = "maxBatchMessages";
	public static final String TIME_FRAME_KEY = "pailstructureTimeFrameInMin";
	public static final String TOPIC_KEY = "topic";
	public static final String PATH_TO_LOCATION_KEY = "path";
	
}
