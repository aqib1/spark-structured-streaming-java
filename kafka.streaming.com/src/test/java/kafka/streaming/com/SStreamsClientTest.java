package kafka.streaming.com;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import kafka.streaming.com.client.SStreamsClient;

public class SStreamsClientTest {

	@Test
	public void initSparkSessionTest() throws Exception {
		SStreamsClient ssClient = null;
		try {
			ssClient = new SStreamsClient().initSparkSession();
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
