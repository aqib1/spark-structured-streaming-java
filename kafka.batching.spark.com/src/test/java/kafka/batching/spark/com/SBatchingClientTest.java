package kafka.batching.spark.com;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class SBatchingClientTest {
	@Test
	public void initSparkSessionTest() throws Exception {
		SBatchingClient ssClient = null;
		try {
			ssClient = new SBatchingClient().initSparkSession();
			Assert.assertNotNull(ssClient.getSpark());
		} catch (IOException | InterruptedException e) {
			throw e;
		} finally {
			try {
				ssClient.getSpark().close();
			} catch (Exception e) {
				throw e;
			}
		}

	}
}
