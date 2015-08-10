package com.vimond.StorageArchitecture.Utils.SparkAccumulator;

import java.util.Date;

import org.apache.spark.AccumulatorParam;

public class MaxDateAccumulatorParam implements  AccumulatorParam<Date>
{

	private static final long serialVersionUID = -5060660044486889448L;

	@Override
	public Date addAccumulator(Date accumulateDate, Date newDate)
	{
		if(accumulateDate.getTime() == 0)//initial case
			return newDate;
		if(accumulateDate.after(newDate))
			return accumulateDate;
		else return newDate;
	}

	@Override
	public Date addInPlace(Date accumulateDate, Date newDate)
	{
		return addAccumulator(accumulateDate, newDate);
	}

	@Override
	public Date zero(Date arg0)
	{
		return new Date(0);
	}
}
