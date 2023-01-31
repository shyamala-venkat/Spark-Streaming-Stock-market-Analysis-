package com.kafka.spark.kafkaSparkAssignment;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class VolumeSorter implements Comparator<Tuple2<String, AverageVolumeCalc>>, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<String, AverageVolumeCalc> o1, Tuple2<String, AverageVolumeCalc> o2) {

		if (Math.abs(o1._2().getVolume()) > Math.abs(o2._2().getVolume())) {
			return 1;
		} else if (Math.abs(o1._2().getVolume()) < Math.abs(o2._2().getVolume())) {
			return -1;
		} else
			return 0;
	}

}
