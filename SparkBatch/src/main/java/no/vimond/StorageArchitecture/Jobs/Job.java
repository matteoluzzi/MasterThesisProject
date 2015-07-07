package no.vimond.StorageArchitecture.Jobs;

import java.io.Serializable;

public interface Job extends Serializable
{
	public void run();
}
