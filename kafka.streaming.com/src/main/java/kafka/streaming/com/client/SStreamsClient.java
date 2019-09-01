package kafka.streaming.com.client;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SStreamsClient {

	private static final String URL_HDFS_FILE = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev";
	private static final String KAFKA_FORMAT = "kafka";
	private static final String KAFKA_BOOTSTRAP_SERVER_KEY = "kafka.bootstrap.servers";
	private static final String SUBSCRIBER_KEY = "subscribe";
	private SparkSession spark = null;
	private static final int BROKER_PORT_NUMBER = 6667;
	private static final String BROKER_DOMAIN_NAME = "sandbox-hdp.hortonworks.com";
	private static String TOPIC_NAME = "BKT_TOPIC";
	private Dataset<Row> datasets;
	private long start;
//	private UserGroupInformation ugi = UserGroupInformation.createRemoteUser("maria_dev");
//	private FileSystem fileSystem;

	public SStreamsClient initSparkSession() throws IOException, InterruptedException {
		spark = SparkSession.builder().appName("SStreamsClient").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		/*
		 * ugi.doAs(new PrivilegedExceptionAction<Void>() {
		 * 
		 * @Override public Void run() throws Exception { Configuration conf = new
		 * Configuration(); conf.set("fs.defaultFS", URL_HDFS_FILE);
		 * conf.set("hadoop.job.ugi", "maria_dev"); fileSystem = FileSystem.get(conf);
		 * // Path outPutPath = new Path(URL_HDFS_FILE); //
		 * fileSystem.create(outPutPath); return null; } });
		 */
		return this;
	}

	public SStreamsClient loadDataSetsFromTopic() {
		start = System.currentTimeMillis();
		System.out.println("Start streaming at [" + (start / 1000) + "] seconds");
		datasets = spark.readStream().format(KAFKA_FORMAT)
				.option(KAFKA_BOOTSTRAP_SERVER_KEY, BROKER_DOMAIN_NAME + ":" + BROKER_PORT_NUMBER)
				.option(SUBSCRIBER_KEY, TOPIC_NAME).load();

		return this;
	}

	public SStreamsClient writeDataSetsToHDFS() throws StreamingQueryException {
		Dataset<Row> s = datasets.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
		StreamingQuery sq = s.writeStream().format("csv").option("path", URL_HDFS_FILE)
				.option("checkpointLocation", "checkpoints").outputMode("complete").start();
		sq.awaitTermination();
		long end = System.currentTimeMillis();
		System.out.println("Time elapsed streaming [" + ((end - start) / 1000) + "]");
		return this;
	}
}
