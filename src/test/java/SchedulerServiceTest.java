import org.junit.Test;
import org.mt.concurrency.SchedulerService;

public class SchedulerServiceTest {

    @Test
    public void testSchedulerServiceWithInitialDelay() throws InterruptedException {
        SchedulerService schedulerService = new SchedulerService();
        schedulerService.scheduleWithInitialDelay(() -> System.out.println("Hii"), 2000);

        Thread.sleep(10000);
    }

    @Test
    public void testSchedulerServiceWithFixedInterval() throws InterruptedException {
        SchedulerService schedulerService = new SchedulerService();
        schedulerService.scheduleWithFixedInterval(() -> System.out.println("Task-1"), 2000, 1000);
        schedulerService.scheduleWithFixedInterval(() -> System.out.println("Task-2"), 2000, 700);

        Thread.sleep(10000);
    }
}
