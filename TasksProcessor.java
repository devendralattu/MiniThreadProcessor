package clarabridge;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;


public class TasksProcessor implements Runnable {
    private static class Customer {
        UUID customerId;
        int taskMinSeconds;
        int taskMaxSeconds;

        public Customer(UUID customerId, int taskMinSeconds, int taskMaxSeconds) {
            this.customerId = customerId;
            this.taskMinSeconds = taskMinSeconds;
            this.taskMaxSeconds = taskMaxSeconds;
        }

        @Override
        public String toString() {
            return new StringBuilder("\n").append("CustomerId = ").append(customerId.toString()).toString();
        }
    }

    private static class Task {
        UUID taskId;
        UUID customerId;
        Date insertedTime;

        public Task(UUID taskId, UUID customerId, Date insertedTime) {
            this.taskId = taskId;
            this.customerId = customerId;
            this.insertedTime = insertedTime;
        }

        @Override
        public String toString() {
            String date = insertedTime != null ? new StringBuilder().append(insertedTime.getHours()).append(":")
                    .append(insertedTime.getMinutes()).append(":").append(insertedTime.getSeconds())
                    .toString() : "no Date recorded yet";
            return new StringBuilder("\n").append("TaskId = ").append(taskId.toString()).append("\t")
                    .append("InsertedTime = ").append(date).toString();
        }
    }

    private Map<UUID, Customer> customerMap;
    private volatile Queue<Task> taskList;
    private volatile Queue<Task> processingList;

    public static void main(String[] args) throws InterruptedException {
        TasksProcessor tasksProcessor = new TasksProcessor();

        Customer c1 = new Customer(UUID.fromString("C1111111-0-0-0-0"), 8, 12);
        Customer c2 = new Customer(UUID.fromString("C2222222-0-0-0-0"), 6, 10);
        Customer c3 = new Customer(UUID.fromString("C3333333-0-0-0-0"), 10, 15);
        Customer c4 = new Customer(UUID.fromString("C4444444-0-0-0-0"), 5, 15);
        Customer c5 = new Customer(UUID.fromString("C5555555-0-0-0-0"), 12, 18);
        Customer c6 = new Customer(UUID.fromString("C6666666-0-0-0-0"), 7, 9);

        tasksProcessor.customerMap = new HashMap<UUID, Customer>() {{
            put(c1.customerId, c1);
            put(c2.customerId, c2);
            put(c3.customerId, c3);
            put(c4.customerId, c4);
            put(c5.customerId, c5);
            put(c6.customerId, c6);
        }};

        Task t1 = new Task(UUID.fromString("A1111111-0-0-0-0"), c1.customerId, null);
        Task t2 = new Task(UUID.fromString("A2222222-0-0-0-0"), c2.customerId, null);
        Task t3 = new Task(UUID.fromString("A3333333-0-0-0-0"), c3.customerId, null);
        Task t4 = new Task(UUID.fromString("A4444444-0-0-0-0"), c4.customerId, null);
        Task t5 = new Task(UUID.fromString("A5555555-0-0-0-0"), c5.customerId, null);
        Task t6 = new Task(UUID.fromString("A6666666-0-0-0-0"), c6.customerId, null);

        tasksProcessor.taskList = new LinkedList<>(Arrays.asList(t1, t2, t3, t4, t5, t6));
        tasksProcessor.processingList = new LinkedList<>();

        // Assuming #threads is low and is equal to the max processing threads on a processing list
        Thread thread1 = new Thread(tasksProcessor);
        Thread thread2 = new Thread(tasksProcessor);
        thread1.start();
        thread2.start();
    }

    @Override
    public void run() {
        while (true) {
            // synchronized (TasksProcessor.class) {
            while (taskList.isEmpty()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.out.println("Invalid taskList empty check");
                }
            }
            // }

            print("Before Processing...");

            Task taskToProcess = taskList.poll();
            processingList.add(taskToProcess);
            Customer customerInfo = customerMap.get(taskToProcess.customerId);

            printProcessingCustomer(customerInfo);

            // print("During processing ..");

            int timeToProcess = (int) (Math.random() * (customerInfo.taskMaxSeconds - customerInfo.taskMinSeconds + 1));
            try {
                Thread.sleep(timeToProcess * 1000);
                taskToProcess.insertedTime = new Date();
            } catch (InterruptedException e) {
                System.out.println("Exception occured while running the Thread");
            } finally {
                // handle any pending process or move tasks out
            }
            taskList.add(processingList.poll());

            print("After processing ..");

            System.err.println("-------------------------------------------------------------------------------------");
        }
    }

    private void printProcessingCustomer(Customer customerInfo) {
        System.out.println("customerToProcess = " + customerInfo.customerId + "\n");
    }

    private void print(String during) {
        System.out.println(during);
        System.out.println("taskList = " + taskList + "\n");
        System.out.println("processingList = " + processingList + "\n");
    }
}
