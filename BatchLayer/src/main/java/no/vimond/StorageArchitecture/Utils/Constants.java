package no.vimond.StorageArchitecture.Utils;

public class Constants
{
	public static final String DEFAULT_ZK_LOCATION = "localhost:2181";
	public static final String DEFAULT_TOPIC = "player-events";
	public static final String DEFAULT_CONSUMER_GROUP_RT = "kafka-consumer-group_rt";
	public static final String DEFAULT_CONSUMER_GROUP_B = "kafka-consumer-group_b";
	
	public static final String PROPERTIES_FILE = "kafkasettings.properties";
	public static final String GROUP_KEY_ID = "group.id";
	
	public static final String IP_STREAM = "ip_stream";
	public static final String EVENT_MESSAGE = "event";
	public static final String IP_MESSAGE = "ip";
	
	public static final String COUNTRY_NAME = "geo.country";
	public static final String LATITUDE = "geo.lat";
	public static final String LONGITUDE = "geo.lon";
	
	public static final long DEFAULT_FLUSH_TIME = 1;
	public static final long MAX_MESSAGES_INTO_FILE = 100;
	
	public static final String MESSAGE_PATH = "messages/";
	public static final String MESSAGE_FILE_NAME = "messages_";
}
