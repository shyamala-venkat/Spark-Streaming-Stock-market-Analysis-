package com.kafka.spark.kafkaSparkAssignment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class DriverMain {
	public static void main(String[] args) throws InterruptedException {

		// Setting the path for winutils.exe
		System.setProperty("hadoop.home.dir", "C:/Hadoop");

		String kafkaBroker = "";
		String topic = "";
		String groupId = "";

		// Passing the arguments from command line
		if (args.length != 3) {
			System.out.println(
					"Please enter all the required values for kafkaBroker,topic and GroupID in the same order");
		} else {

			kafkaBroker = args[0];
			topic = args[1];
			groupId = args[2];

			// Initializing the spark configuration so as to run in the local
			// machine
			SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[*]");

			// Setting the below value so as to increase the polling time for
			// Sparkconf
			sparkConf.set("spark.streaming.kafka.consumer.poll.ms", "2000");

			// Creating the Spark Streaming context which is the starting point
			// of our application with a batch interval of 1 minute

			JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.minutes(1));

			// Setting the log level to Warnings
			jssc.sparkContext().setLogLevel("WARN");

			// Creating HashMap to hold Kafka related parameters
			Map<String, Object> KafkaParams = new HashMap<>();

			// Creating HashSet to hold the Kafka topic where data is hosted
			Set<String> topic1 = new HashSet<>(Arrays.asList(topic));

			// Populating the KafkaParams HashMap to hold the key,value pairs
			// needed for consumer configuration

			KafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
			KafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			KafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			KafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

			// Connecting point for Kafka and Spark Streaming. Creating a
			// DStream by subscribing to the given Kafka Broker Address and
			// topic and to hold the incoming streaming data from Kafka

			JavaInputDStream<ConsumerRecord<String, JsonNode>> messages = org.apache.spark.streaming.kafka010.KafkaUtils
					.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
							ConsumerStrategies.<String, JsonNode>Subscribe(topic1, KafkaParams));

			// Mapping the incoming DStream to CryptoCurrency class in order to
			// map the json to CryptoCurrency object in a sliding window of 5
			// minutes and window period of 10 minutes

			JavaDStream<CryptoCurrency> lines = messages
					.map(new Function<ConsumerRecord<String, JsonNode>, CryptoCurrency>() {

						private static final long serialVersionUID = 1L;

						@Override
						public CryptoCurrency call(ConsumerRecord<String, JsonNode> record) throws Exception {
							ObjectMapper objectMapper = new ObjectMapper();
							CryptoCurrency cr = objectMapper.treeToValue(record.value(), CryptoCurrency.class);
							System.out.println("cr in string ::" + cr.toString());
							return cr;
						}
					}).window(Durations.minutes(10), Durations.minutes(5));

			// Mapping the DStream to PairDStream so as to use for reduceByKey()
			// method. The key used is symbol of the cryptocurrency and the
			// value used will be a class having all the values required for the
			// calculation of averages and total trading volume (count,open
			// prize,close prize, volume)

			JavaPairDStream<String, AverageVolumeCalc> pair = lines
					.flatMapToPair(new PairFlatMapFunction<CryptoCurrency, String, AverageVolumeCalc>() {

						private static final long serialVersionUID = 67676744;

						@Override
						public Iterator<Tuple2<String, AverageVolumeCalc>> call(CryptoCurrency t) throws Exception {
							List<Tuple2<String, AverageVolumeCalc>> list = new ArrayList<Tuple2<String, AverageVolumeCalc>>();

							PriceData p = t.getPriceData();

							list.add(new Tuple2<String, AverageVolumeCalc>(t.getSymbol(),
									new AverageVolumeCalc(1, p.getClose(), p.getOpen(), p.getVolume())));

							return list.iterator();
						}
					});

			// Applying reduceByKey() transformation on the above PairDStream so
			// as to aggregate the values needed for average and volume
			// calculation

			JavaPairDStream<String, AverageVolumeCalc> result = pair
					.reduceByKey(new Function2<AverageVolumeCalc, AverageVolumeCalc, AverageVolumeCalc>() {

						private static final long serialVersionUID = 76761212;

						public AverageVolumeCalc call(AverageVolumeCalc data1, AverageVolumeCalc data2)
								throws Exception {
							data1.setCloseAverage(data1.getCloseAverage() + data2.getCloseAverage());
							data1.setCount(data1.getCount() + data2.getCount());
							data1.setOpenAverage(data1.getOpenAverage() + data2.getOpenAverage());
							data1.setVolume(Math.abs(data1.getVolume()) + Math.abs(data2.getVolume()));
							return data1;
						}

					});

			// Converting the above PairDStream to PairRDD and applying the
			// formula in AverageVolumeCalc class to get the required results

			result.foreachRDD(new VoidFunction<JavaPairRDD<String, AverageVolumeCalc>>() {

				private static final long serialVersionUID = 6767679;

				public void call(JavaPairRDD<String, AverageVolumeCalc> t) throws Exception, NumberFormatException {

					t.foreach(data -> {

						System.out.println("Symbol :: " + data._1() + "count :: " + data._2().getCount()
								+ " ClosingAverage :: " + data._2().getCloseAverage());

						System.out.println("The Simple Moving Average for symbol " + data._1() + " is "
								+ data._2().closeAverage());
					});
					

					// Using the custom comparator to sort the difference of
					// average close price and average open price values for
					// each symbol in descending order

					List<Tuple2<String, AverageVolumeCalc>> ls = t.top(4, new AverageSorter());

					System.out.println("The difference between Average closing and Average open price for the symbol "
							+ ls.get(0)._1() + " is "
							+ (Double.valueOf(ls.get(0)._2().closeAverage()) - (ls.get(0)._2().openAverage()))
							+ " with Average closing price as " + ls.get(0)._2().getCloseAverage()
							+ " and Average opening price as " + ls.get(0)._2().getOpenAverage());

					System.out.println("The difference between Average closing and Average open price for the symbol "
							+ ls.get(1)._1() + " is "
							+ (Double.valueOf(ls.get(1)._2().closeAverage()) - (ls.get(1)._2().openAverage()))
							+ " with Average closing price as " + ls.get(1)._2().getCloseAverage()
							+ " and Average opening price as " + ls.get(1)._2().getOpenAverage());

					System.out.println("The difference between Average closing and Average open price for the symbol "
							+ ls.get(2)._1() + " is "
							+ (Double.valueOf(ls.get(2)._2().closeAverage()) - (ls.get(2)._2().openAverage()))
							+ " with Average closing price as " + ls.get(2)._2().getCloseAverage()
							+ " and Average opening price as " + ls.get(2)._2().getOpenAverage());

					System.out.println("The difference between Average closing and Average open price for the symbol "
							+ ls.get(3)._1() + " is "
							+ (Double.valueOf(ls.get(3)._2().closeAverage()) - (ls.get(3)._2().openAverage()))
							+ " with Average closing price as " + ls.get(3)._2().getCloseAverage()
							+ " and Average opening price as " + ls.get(3)._2().getOpenAverage());

					System.out.println(
							"The maximum profit yielding stock is " + ls.get(0)._1() + " giving maximum profit as "
									+ (Double.valueOf(ls.get(0)._2().closeAverage()) - (ls.get(0)._2().openAverage())));

					// Using the custom comparator to sort the total traded
					// volume for each symbol in descending order

					List<Tuple2<String, AverageVolumeCalc>> ls1 = t.top(4, new VolumeSorter());

					System.out.println("The symbol " + ls1.get(0)._1() + " has total traded volums as "
							+ ls1.get(0)._2().getVolume());
					System.out.println("The symbol " + ls1.get(1)._1() + " has total traded volums as "
							+ ls1.get(1)._2().getVolume());
					System.out.println("The symbol " + ls1.get(2)._1() + " has total traded volums as "
							+ ls1.get(2)._2().getVolume());
					System.out.println("The symbol " + ls1.get(3)._1() + " has total traded volums as "
							+ ls1.get(3)._2().getVolume());

					System.out.println("The most recommended stock to be bought out of the four given stocks is "
							+ ls1.get(0)._1());
				}

			});

			jssc.start();
			jssc.awaitTermination();

		}
	}
}
