package kafka.batching.spark.com;

import java.io.IOException;

import org.apache.spark.sql.streaming.StreamingQueryException;


public class MainClass {
	public static void main(String[] args) {
		try {
			new SBatchingClient().initSparkSession().loadDataSetsFromTopic().writeDataSetsToHDFS();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
