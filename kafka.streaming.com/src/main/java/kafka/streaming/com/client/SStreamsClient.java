package kafka.streaming.com.client;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SStreamsClient {
//172.18.0.2
	// hdfs://sandbox-hdp.hortonworks.com:8020
	private static final String URL_HDFS_FILE = "/user/sparktest/hdfs";
	private static final String KAFKA_FORMAT = "kafka";
	private static final String KAFKA_BOOTSTRAP_SERVER_KEY = "kafka.bootstrap.servers";
	private static final String SUBSCRIBER_KEY = "subscribe";
	private static final int BROKER_PORT_NUMBER = 6667;
	private static final String BROKER_DOMAIN_NAME = "sandbox-hdp.hortonworks.com";
	private static final String SPARK_SQL_STREAMING_CHECKPOINT_LOCATION_CONFIG = "spark.sql.streaming.checkpointLocation";
	private static final String SPARK_SQL_STREAMING_CHECKPOINT_LOCATION = "/user/sparktest/checkpoints";
	private static final String SPAKR_STREAM_STARTING_OFFSET_KEY = "startingOffsets";
	private static final String SPARK_STREAM_STARTING_OFFSET_BEGINNING = "earliest";
	private static final long DEFAULT_STOP_STREAMING_TIMEINTERVAL = 60000l;
	private SparkSession spark = null;
	private boolean isStreamContinue = false;
	private static String TOPIC_NAME = "MTKAF";
	private Dataset<Row> datasets;
	private long start;

	/**
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public SStreamsClient initSparkSession() throws IOException, InterruptedException {

		spark = SparkSession.builder().master("local[*]").appName("SStreamsClient")
				.config(SPARK_SQL_STREAMING_CHECKPOINT_LOCATION_CONFIG, SPARK_SQL_STREAMING_CHECKPOINT_LOCATION)
				.getOrCreate();
		return this;
	}

	/**
	 * @return
	 */
	public SStreamsClient loadDataSetsFromTopic() {
		start = System.currentTimeMillis();
		System.out.println("Current time in seconds [" + (start / 1000) + "] seconds");
		datasets = spark.readStream().format(KAFKA_FORMAT)
				.option(KAFKA_BOOTSTRAP_SERVER_KEY, BROKER_DOMAIN_NAME + ":" + BROKER_PORT_NUMBER)
				.option(SUBSCRIBER_KEY, TOPIC_NAME)
				.option(SPAKR_STREAM_STARTING_OFFSET_KEY, SPARK_STREAM_STARTING_OFFSET_BEGINNING).load();

		return this;
	}

	public SStreamsClient continueStream() {
		isStreamContinue = true;
		return this;
	}

	/**
	 * @return
	 * @throws StreamingQueryException
	 */
	public SStreamsClient writeDataSetsToHDFS() throws StreamingQueryException {
		writeDataSets(DEFAULT_STOP_STREAMING_TIMEINTERVAL);
		return this;
	}

	public SStreamsClient writeDataSetsToHDFS(long interval) throws StreamingQueryException {
		writeDataSets(interval);
		return this;
	}

	private void writeDataSets(long interval) throws StreamingQueryException {
		Dataset<Row> s = datasets.selectExpr("CAST(value AS STRING)");
		StreamingQuery sq = s.writeStream().format("csv").option("path", URL_HDFS_FILE).outputMode(OutputMode.Append())
				.start();

		if (isStreamContinue)
			sq.awaitTermination();
		else
			sq.awaitTermination(interval);

		long end = System.currentTimeMillis();
		System.out.println("Time after completing streaming [" + ((end - start) / 1000) + "] seconds");
	}
}
