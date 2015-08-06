package no.vimond.RealTimeArchitecture.Utils;

public class Constants
{
	public static final String DEFAULT_ZK_LOCATION = "localhost";
	public static final long DEFAULT_ZK_PORT = 2181;
	public static final String DEFAULT_TOPIC = "player-events";
	public static final String DEFAULT_CONSUMER_GROUP_RT = "kafka-consumer-group_rt";
	public static final Boolean DEFAULT_LOCAL_MODE = false;
	public static final String DEFAULT_ZK_BROKER_PATH = "zookeeper.brokerPath";
	public static String DEFAULT_ES_INDEX = "vimond-realtime/realtime-player-events";
	
	public static final String PROPERTIES_FILE = "";
	public static final String GROUP_KEY_ID = "group.id";
	
	public static final String IP_STREAM = "ip_stream";
	public static final String EVENT_MESSAGE = "event";
	public static final String IP_MESSAGE = "ip";
	
	public static final String COUNTRY_NAME = "data.geo.country";
	public static final String LATITUDE = "data.geo.lat";
	public static final String LONGITUDE = "data.geo.lon";
	public static final String CITY = "data.geo.city";
	public static final String APPNAME = "data.appName";
	public static final String ASSETID = "data.assetId";
	
	public static final String SPOUT = "kafka-spout";
	public static final String BOLT_ROUTER = "router-bolt";
	public static final String BOLT_GEOIP = "geoip-bolt";
	public static final String BOLT_ES = "es-bolt";


	
	
	
}
