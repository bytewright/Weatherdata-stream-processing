package de.bytewright.demo.flinkstreaming.kafka;

import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.time.LocalDate;
import java.util.concurrent.CountDownLatch;

public class KafkaProducerCloser implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerCloser.class);
    private final KafkaProducer<LocalDate, ClimateData> producer;
    private final CountDownLatch countDownLatch;

    public KafkaProducerCloser(KafkaProducer<LocalDate, ClimateData> producer, CountDownLatch countDownLatch) {
        this.producer = producer;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        try {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            LOGGER.info("Starting to wait for senders to finish...");
            countDownLatch.await();
            LOGGER.info("All senders are finished, closing producer");
            producer.close();
            stopWatch.stop();
            LOGGER.info("Closed producer after a total of {}s", stopWatch.lastTaskInfo().getTimeSeconds());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
