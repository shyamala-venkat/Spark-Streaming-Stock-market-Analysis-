package com.kafka.spark.kafkaSparkAssignment;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PriceData implements Serializable {

	public static final long serialVersionUID = 1L;

	@JsonProperty("close")
	private float close;
	@JsonProperty("high")
	private float high;
	@JsonProperty("low")
	private float low;
	@JsonProperty("open")
	private float open;
	@JsonProperty("volume")
	private double volume;

	public float getClose() {
		return close;
	}

	public void setClose(float close) {
		this.close = close;
	}

	public float getHigh() {
		return high;
	}

	public void setHigh(float high) {
		this.high = high;
	}

	public float getLow() {
		return low;
	}

	public void setLow(float low) {
		this.low = low;
	}

	public float getOpen() {
		return open;
	}

	public void setOpen(float open) {
		this.open = open;
	}

	public double getVolume() {
		return volume;
	}

	public void setVolume(double volume) {
		this.volume = volume;
	}

	@Override
	public String toString() {
		return "priceData [close=" + close + ", high=" + high + ", low=" + low + ", open=" + open + ", volume=" + volume
				+ "]";
	}

}
