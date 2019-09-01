package kafka.streaming.com.main;

import java.io.IOException;

import org.apache.spark.sql.streaming.StreamingQueryException;

import kafka.streaming.com.client.SStreamsClient;

public class MainClass {

	public static void main(String[] args) {
		try {
			new SStreamsClient().initSparkSession().loadDataSetsFromTopic().writeDataSetsToHDFS();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
