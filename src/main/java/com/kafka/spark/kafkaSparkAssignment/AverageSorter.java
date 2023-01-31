package com.kafka.spark.kafkaSparkAssignment;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class AverageSorter implements Comparator<Tuple2<String, AverageVolumeCalc>>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<String, AverageVolumeCalc> o1, Tuple2<String, AverageVolumeCalc> o2) {

		if (((o1._2().closeAverage()) - (o1._2().openAverage())) > ((o2._2().closeAverage())
				- (o2._2().openAverage()))) {
			return 1;
		} else if (((o2._2().closeAverage()) - (o2._2().openAverage())) > ((o1._2().closeAverage())
				- (o1._2().openAverage()))) {
			return -1;
		} else
			return 0;
	}

}
