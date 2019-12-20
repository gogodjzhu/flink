import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;

/**
 * 启动JobManager & TaskManager
 *
 * @author DJ.Zhu
 */
public class LocalServer {

	private static final String configDir = LocalServer.class.getResource("/").getPath();
	private static final Integer taskManagerCnt = 3;

    public static void main(String[] args) throws InterruptedException {
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		CountDownLatch countDownLatch = new CountDownLatch(1 + taskManagerCnt);
		executorService.submit(() -> {
			StandaloneSessionClusterEntrypoint.main(new String[]{"-c", configDir});
			countDownLatch.countDown();
		});
		for (int i = 0; i < taskManagerCnt; i ++) {
			executorService.submit(() -> {
				try {
					TaskManagerRunner.main(new String[]{"-c", configDir});
				} catch (Exception e) {
					e.printStackTrace();
				}
				countDownLatch.countDown();
			});
		}
		countDownLatch.await();
    }

}
