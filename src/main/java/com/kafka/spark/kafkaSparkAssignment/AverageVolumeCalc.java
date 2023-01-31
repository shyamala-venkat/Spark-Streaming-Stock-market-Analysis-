package com.kafka.spark.kafkaSparkAssignment;

import java.io.Serializable;

public class AverageVolumeCalc implements Serializable {
		/**
		*
		*/
		private static final long serialVersionUID = 332323;
		private int count;
		private float closeAverage;
		private float openAverage;
		private String maxAvgProfitSymbol;
		private double volume;
		
		
		public String getMaxAvgProfitSymbol() {
			return maxAvgProfitSymbol;
		}
		public void setMaxAvgProfitSymbol(String maxAvgProfitSymbol) {
			this.maxAvgProfitSymbol = maxAvgProfitSymbol;
		}
	
		public float getCloseAverage() {
			return closeAverage;
		}
		public void setCloseAverage(float closeAverage) {
			this.closeAverage = closeAverage;
		}
		public float getOpenAverage() {
			return openAverage;
		}
		public void setOpenAverage(float openAverage) {
			this.openAverage = openAverage;
		}
		public double getVolume() {
			return volume;
		}
		public void setVolume(double volume) {
			this.volume = volume;
		}
		
		public AverageVolumeCalc(int count, float average,float openAverage,double volume) {
			super();
			this.count = count;
			this.closeAverage = average;
			this.openAverage = openAverage;
			this.volume = volume;
		}
		
		public int getCount() {
		return count;
		}
		public void setCount(int count) {
		this.count = count;
		}
		
		public double openAverage() {
			
			return (openAverage/count);
		}
		public double closeAverage(){
			return (closeAverage/count);
		}
		@Override
		public String toString() {
		return String.valueOf(closeAverage/count);
		}
}
