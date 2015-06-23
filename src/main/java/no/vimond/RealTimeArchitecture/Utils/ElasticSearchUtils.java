package no.vimond.RealTimeArchitecture.Utils;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;

public class ElasticSearchUtils
{

	private static Node clusterNode = nodeBuilder().local(true).node();

	public static Client getELClient()
	{
		if (clusterNode != null)
		{
			return clusterNode.client();
		}
		else
			return null;
	}

}
