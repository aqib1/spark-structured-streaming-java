package kafka.batch.spark.com;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * @author Aqib_Javed
 * this class uses read method of spark session which is used to read non-stream data 
 * in the form of batch
 */
public class SBatchClient {
	private static final String URL_HDFS_FILE = "/user/sparkbatchtest/hdfs";
	private static final String KAFKA_FORMAT = "kafka";
	private static final String KAFKA_BOOTSTRAP_SERVER_KEY = "kafka.bootstrap.servers";
	private static final String SUBSCRIBER_KEY = "subscribe";
	private static final int BROKER_PORT_NUMBER = 6667;
	private static final String BROKER_DOMAIN_NAME = "sandbox-hdp.hortonworks.com";
	private static final String SPARK_SQL_STREAMING_CHECKPOINT_LOCATION_CONFIG = "spark.sql.streaming.checkpointLocation";
	private static final String SPARK_SQL_STREAMING_CHECKPOINT_LOCATION = "/user/sparkbatchtest/checkpoints";
	private static final String SPAKR_STREAM_STARTING_OFFSET_KEY = "startingOffsets";
	private static final String SPARK_STREAM_STARTING_OFFSET_BEGINNING = "earliest";
	private SparkSession spark = null;
	private static String TOPIC_NAME = "MTKAF";
	private Dataset<Row> datasets;
	private long start;

	/**
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public SBatchClient initSparkSession() throws IOException, InterruptedException {
		spark = SparkSession.builder().master("local[*]").appName("SStreamsClient")
				.config(SPARK_SQL_STREAMING_CHECKPOINT_LOCATION_CONFIG, SPARK_SQL_STREAMING_CHECKPOINT_LOCATION)
				.getOrCreate();
		return this;
	}

	/**
	 * @return
	 */
	public SBatchClient loadDataSetsFromTopic() {
		start = System.currentTimeMillis();
		System.out.println("Current time in seconds [" + (start / 1000) + "] seconds");
		datasets = spark.read().format(KAFKA_FORMAT)
				.option(KAFKA_BOOTSTRAP_SERVER_KEY, BROKER_DOMAIN_NAME + ":" + BROKER_PORT_NUMBER)
				.option(SUBSCRIBER_KEY, TOPIC_NAME)
				.option(SPAKR_STREAM_STARTING_OFFSET_KEY, SPARK_STREAM_STARTING_OFFSET_BEGINNING).load();
		return this;
	}

	/**
	 * @return
	 * @throws StreamingQueryException
	 */
	public SBatchClient writeDataSetsToHDFS() throws StreamingQueryException {
		writeDataSets();
		return this;
	}

	/**
	 * @return
	 */
	public SparkSession getSpark() {
		return spark;
	}

	/**
	 * @param interval
	 * @throws StreamingQueryException
	 * 
	 * calculating time in the end
	 * 
	 */
	private void writeDataSets() throws StreamingQueryException {
		Dataset<Row> s = datasets.selectExpr("CAST(value AS STRING)");
		s.write().format("csv").option("path", URL_HDFS_FILE).save();

		long end = System.currentTimeMillis();
		System.out.println("Time after completing streaming [" + ((end - start) / 1000) + "] seconds");
	}
}
