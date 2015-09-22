package com.vimond.utils.data;

public class Constants
{
	public static final String DEFAULT_ZK_LOCATION = "localhost";
	public static final long DEFAULT_ZK_PORT = 2181;
	public static final String DEFAULT_TOPIC = "player-events";
	public static final String DEFAULT_CONSUMER_GROUP_RT = "kafka-consumer-group_rt";
	public static final Boolean DEFAULT_LOCAL_MODE = false;
	public static final String DEFAULT_ZK_BROKER_PATH = "zookeeper.brokerPath";
	public static final String DEFAULT_ES_INDEX = "vimond-realtime/realtime-player-events";
	public static final int DEFAULT_METRIC_FREQUENCY = 30;
	public static final String DEFAULT_METRIC_PATH = "/Users/matteoremoluzzi/report";
	public static final boolean DEFAULT_ACKING_MODE = false;
	
	public static final String PROPERTIES_FILE = "app.properties";
	public static final String GROUP_KEY_ID = "group.id";
	
	public static final String IP_STREAM = "ip_stream";
	public static final String UA_STREAM = "ua_stream";
	public static final String EVENT_MESSAGE = "event";
	public static final String IP_MESSAGE = "ip";
	public static final String TIMEINIT = "timeinit";
	
	public static final String COUNTRY_NAME = "data.geo.country";
	public static final String LATITUDE = "data.geo.lat";
	public static final String LONGITUDE = "data.geo.lon";
	public static final String CITY = "data.geo.city";
	public static final String APPNAME = "data.appName";
	public static final String ASSETID = "data.assetId";
	public static final String BROWSER = "data.browser";
	public static final String OS = "data.os";
	public static final String COUNTER = "counter";
	
	public static final String SPOUT = "kafka-spout";
	public static final String BOLT_ROUTER = "router-bolt";
	public static final String BOLT_GEOIP = "geoip-bolt";
	public static final String BOLT_ES = "es-bolt";
	public static final String BOLT_USER_AGENT = "useragent-bolt";
	public static final String BOLT_SERIALIZER = "serializer-bolt";
	
	public static final String APP_NAME_KEY = "app.name";
	public static final String DB_LITE_KEY = "MaxMindLiteVersion";
	public static final String POOL_SIZE_KEY = "executorThreads";

	public static final String NEW_DATA_PATH = "/Users/matteoremoluzzi/dataset/newData";

	public static final String MASTER_DATA_PATH = "/Users/matteoremoluzzi/dataset/masterData";
	public static final int DEFAULT_STORM_BATCH_SIZE = 1000;
	
	public static final int SPOUT_TASKS = 3;
	public static final int EL_TASKS = 3;
	public static final int ROUTER_TASKS = 2;
	public static final int USER_AGENT_TASKS = 2;
	public static final int SERIALIZER_TASKS = 2;
	
}
