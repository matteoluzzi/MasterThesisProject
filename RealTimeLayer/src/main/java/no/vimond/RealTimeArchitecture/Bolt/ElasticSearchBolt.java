package no.vimond.RealTimeArchitecture.Bolt;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_BATCH_FLUSH_MANUAL;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_BATCH_SIZE_ENTRIES;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE;
import static org.elasticsearch.storm.cfg.StormConfigurationOptions.ES_STORM_BOLT_ACK;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.PartitionWriter;
import org.elasticsearch.storm.TupleUtils;
import org.elasticsearch.storm.cfg.StormSettings;
import org.elasticsearch.storm.serialization.StormTupleBytesConverter;
import org.elasticsearch.storm.serialization.StormTupleFieldExtractor;
import org.elasticsearch.storm.serialization.StormValueWriter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
/**
 * Modified version of the standard implementation provided by elasticsearch api.
 * Correction of a bug at line 130 and 136 (removed negation mark on <code>ackWrites</code> variable) 
 * and other stuff
 * @author matteoremoluzzi
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ElasticSearchBolt implements IRichBolt
{

	private static final long serialVersionUID = 3656656932100012895L;

	private transient static Log log = LogFactory
			.getLog(ElasticSearchBolt.class);
	
	private static final Logger LOGGER = LogManager.getLogger(ElasticSearchBolt.class);

	private Map boltConfig = new LinkedHashMap();

	private transient PartitionWriter writer;
	private transient boolean flushOnTickTuple = true;
	private transient boolean ackWrites = false;

	private transient List<Tuple> inflightTuples = null;
	private transient int numberOfEntries = 0;
	private transient OutputCollector collector;

	public ElasticSearchBolt(String target)
	{
		boltConfig.put(ES_RESOURCE_WRITE, target);
	}

	public ElasticSearchBolt(String target, boolean writeAck)
	{
		boltConfig.put(ES_RESOURCE_WRITE, target);
		boltConfig.put(ES_STORM_BOLT_ACK, Boolean.toString(writeAck));
	}

	public ElasticSearchBolt(String target, Map configuration)
	{
		boltConfig.putAll(configuration);
		boltConfig.put(ES_RESOURCE_WRITE, target);
	}

	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector)
	{
		this.collector = collector;

		LinkedHashMap copy = new LinkedHashMap(conf);
		copy.putAll(boltConfig);

		StormSettings settings = new StormSettings(copy);
		flushOnTickTuple = settings.getStormTickTupleFlush();
		ackWrites = settings.getStormBoltAck();

		// trigger manual flush
		if (ackWrites)
		{
			settings.setProperty(ES_BATCH_FLUSH_MANUAL, Boolean.TRUE.toString());

			// align Bolt / es-hadoop batch settings
			numberOfEntries = settings.getStormBulkSize();
			settings.setProperty(ES_BATCH_SIZE_ENTRIES,
					String.valueOf(numberOfEntries));

			inflightTuples = new ArrayList<Tuple>(numberOfEntries + 1);
		}

		int totalTasks = context
				.getComponentTasks(context.getThisComponentId()).size();

		InitializationUtils.setValueWriterIfNotSet(settings,
				StormValueWriter.class, log);
		InitializationUtils.setBytesConverterIfNeeded(settings,
				StormTupleBytesConverter.class, log);
		InitializationUtils.setFieldExtractorIfNotSet(settings,
				StormTupleFieldExtractor.class, log);

		writer = RestService.createWriter(settings, context.getThisTaskIndex(),
				totalTasks, log);
	}

	public void execute(Tuple input)
	{

		if (flushOnTickTuple && TupleUtils.isTickTuple(input))
		{
			flush();
			return;
		}
		if (ackWrites)
		{
			inflightTuples.add(input);
		}
		try
		{
			long  initTime = (Long) input.getValue(1);
			input.getValues().remove(1);
			writer.repository.writeToIndex(input);
			LOGGER.info("Received message with init time : " + initTime);
			// manual flush in case of ack writes - handle it here.
			if (numberOfEntries > 0 && inflightTuples.size() >= numberOfEntries)
			{
				flush();
			}

			if (ackWrites)
			{
				collector.ack(input);
			}
		} catch (RuntimeException ex)
		{
			if (ackWrites)
			{
				collector.fail(input);
			}
			throw ex;
		}
		LOGGER.info("Processed message");
	}

	private void flush()
	{
		if (ackWrites)
		{
			flushWithAck();
		} else
		{
			flushNoAck();
		}
	}

	private void flushWithAck()
	{
		BitSet flush = null;

		try
		{
			flush = writer.repository.tryFlush();
			writer.repository.discard();
		} catch (EsHadoopException ex)
		{
			// fail all recorded tuples
			for (Tuple input : inflightTuples)
			{
				collector.fail(input);
			}
			inflightTuples.clear();
			throw ex;
		}

		for (int index = 0; index < inflightTuples.size(); index++)
		{
			Tuple tuple = inflightTuples.get(index);
			// bit set means the entry hasn't been removed and thus wasn't
			// written to ES
			if (flush.get(index))
			{
				collector.fail(tuple);
			} else
			{
				collector.ack(tuple);
			}
		}

		// clear everything in bulk to prevent 'noisy' remove()
		inflightTuples.clear();
	}

	private void flushNoAck()
	{
		writer.repository.flush();
	}

	public void cleanup()
	{
		if (writer != null)
		{
			try
			{
				flush();
			} finally
			{
				writer.close();
				writer = null;
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}