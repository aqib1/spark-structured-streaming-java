package kafka.batch.spark.com;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class SBatchClientTest {
	@Test
	public void initSparkSessionTest() throws Exception {
		SBatchClient ssClient = null;
		try {
			ssClient = new SBatchClient().initSparkSession();
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

