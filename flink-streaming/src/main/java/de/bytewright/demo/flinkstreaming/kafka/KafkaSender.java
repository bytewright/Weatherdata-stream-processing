package de.bytewright.demo.flinkstreaming.kafka;

import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class KafkaSender implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);
    private final int index;
    private final KafkaProducer<LocalDate, ClimateData> producer;
    private final Iterator<ClimateData> iterator;
    private final CountDownLatch countDownLatch;

    public KafkaSender(int index, List<ClimateData> climateData, KafkaProducer<LocalDate, ClimateData> producer, CountDownLatch countDownLatch) {
        this.index = index;
        this.producer = producer;
        this.iterator = climateData.iterator();
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        while (iterator.hasNext()) {
            ClimateData climateDataRow = iterator.next();
            LocalDate key = climateDataRow.getRecordingDate();
            LOGGER.debug("kafka producer {}: sending data row with key: {}", index, key);
            var record = new ProducerRecord<>("flink_input", key, climateDataRow);
            producer.send(record);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        LOGGER.info("kafka producer {}: all data sent, shutting down", index);
        countDownLatch.countDown();
    }
}
