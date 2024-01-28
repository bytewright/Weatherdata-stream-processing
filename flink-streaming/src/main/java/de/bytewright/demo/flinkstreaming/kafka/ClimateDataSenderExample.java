package de.bytewright.demo.flinkstreaming.kafka;

import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateData;
import org.apache.flink.shaded.guava31.com.google.common.io.Files;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Component
public class ClimateDataSenderExample implements InitializingBean, DisposableBean {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ClimateDataSenderExample.class);
    private ExecutorService executorService;

    public void start() {
        try {
            ClassLoader classLoader = this.getClass().getClassLoader();
            Path path = Path.of(classLoader.getResource("ClimateData.txt").toURI());
            List<String> lines = Files.readLines(path.toFile(), StandardCharsets.UTF_8);
            Map<Integer, List<ClimateData>> collect = lines.stream()
                    .map(ClimateData::from)
                    .filter(climateData -> climateData.getRecordingDate() != null)
                    .collect(Collectors.groupingBy(this::getWeekday));
            LOGGER.info("Splitted all climate data by day of week, found data for {} threads", collect.size());
            KafkaProducer<LocalDate, ClimateData> producer = new KafkaProducer<>(createKafkaProperties());
            CountDownLatch countDownLatch = new CountDownLatch(collect.size());
            executorService.execute(new KafkaProducerCloser(producer, countDownLatch));
            for (int i = 1; i <= collect.size(); i++) {
                List<ClimateData> climateData = collect.get(i);
                Runnable sender = new KafkaSender(i, climateData, producer, countDownLatch);
                executorService.execute(sender);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Integer getWeekday(ClimateData climateData) {
        LocalDate recordingDate = climateData.getRecordingDate();
        return recordingDate.get(ChronoField.DAY_OF_WEEK);
    }

    private Properties createKafkaProperties() {
        // Create the properties for the producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Kafka broker address
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Serializer class for the key
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Serializer class for the value
        props.put("acks", "all"); // Acknowledgement level
        return props;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        executorService = Executors.newFixedThreadPool(7);
    }

    @Override
    public void destroy() throws Exception {

    }
}
