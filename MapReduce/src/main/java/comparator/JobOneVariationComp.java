package comparator;

import java.util.Comparator;

import outputobjects.JobOneOutOne;

public class JobOneVariationComp implements Comparator<JobOneOutOne>{

	@Override
	public int compare(JobOneOutOne arg0, JobOneOutOne arg1) {
		return arg0.getVariation().compareTo(arg1.getVariation());
	}

}
