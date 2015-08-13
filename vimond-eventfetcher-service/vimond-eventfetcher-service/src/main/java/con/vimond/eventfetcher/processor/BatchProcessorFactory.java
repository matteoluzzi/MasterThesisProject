package con.vimond.eventfetcher.processor;

/**
 * Factory class for creating a different message processor according to the input parameter of createMessageProcessor method.<br>
 * Usage of singleton pattern
 * @author matteoremoluzzi
 *
 */
public class BatchProcessorFactory
{
	private static BatchProcessorFactory instance = new BatchProcessorFactory();
	
	private BatchProcessorFactory()
	{
	}
	
	public static BatchProcessorFactory getFactory()
	{
		return instance;
	}
	
	public BatchProcessor createMessageProcessor(BatchProcessorEnum type)
	{
		switch (type)
		{
		case VIMOND:
			return new VimondEventAnyMessageProcessor();
		case STRING:
			return new StringMessageProcessor();
		default:
			return new StringMessageProcessor();
		}
	}
	
}
