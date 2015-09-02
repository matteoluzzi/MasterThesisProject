package com.vimond.StorageArchitecture.Utils;

import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;
import com.vimond.utils.data.SimpleModel;
import com.vimond.utils.data.SparkEvent;

/**
 * Class for registering custom kyro class serialization for memory performances tuning
 * @author matteoremoluzzi
 *
 */
public class ClassRegistrator implements KryoRegistrator
{
	@Override
	public void registerClasses(Kryo registrator)
	{
		//input model
		registrator.register(SparkEvent.class);
		//output model
		registrator.register(SimpleModel.class);
	}
}
