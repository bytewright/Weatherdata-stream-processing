package de.bytewright.demo.flinkstreaming.climatedata;

import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateData;
import de.bytewright.demo.flinkstreaming.climatedata.dto.ClimateRecord;
import de.bytewright.demo.flinkstreaming.climatedata.dto.WeekClimateRecord;
import de.bytewright.demo.flinkstreaming.climatedata.flinkcomponents.CalendarWeekWindowAssigner;
import de.bytewright.demo.flinkstreaming.climatedata.flinkcomponents.WeekClimateDataProcessor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.slf4j.Logger;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.WeekFields;

public class ClimateDataProcessingJob implements Serializable {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ClimateDataProcessingJob.class);

    public void start(StreamExecutionEnvironment env) {

        FileSource<String> dataSource = getDataSource("ClimateData.txt");
        DataStreamSource<String> source = env.fromSource(dataSource, WatermarkStrategy.noWatermarks(), "climateDataFiles");
        source.map(ClimateData::from)
                .filter(value -> value.getRecordingDate() != null)
                .map(this::toClimateRecord)
                .assignTimestampsAndWatermarks(climateDataTimeStampExtractor())
                .windowAll(new CalendarWeekWindowAssigner())
                .process(new WeekClimateDataProcessor())
                .sinkTo(getFileSink())
                .setParallelism(1);

    }

    private FileSink<WeekClimateRecord> getFileSink() {
        OutputFileConfig outputFileConfig = OutputFileConfig.builder()
                .withPartPrefix("climateDataByWeek")
                .build();
        return FileSink.forRowFormat(new Path("out"), stringEncoder())
                .withOutputFileConfig(outputFileConfig)
                .build();
    }

    private Encoder<WeekClimateRecord> stringEncoder() {
        return (element, stream) -> {
            double avg = element.tempSum() / element.dataCount();
            int weekOfYear = element.minTime().get(WeekFields.ISO.weekOfYear());
            int year = element.minTime().getYear();
            String formatted = String.format("%d-%02d; %02f\n", year, weekOfYear, avg);
            stream.write(formatted.getBytes(StandardCharsets.UTF_8));
        };
    }

    private WatermarkStrategy<ClimateRecord> climateDataTimeStampExtractor() {
        return new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new BoundedOutOfOrdernessTimestampExtractor<>(Time.days(10)) {
            @Override
            public long extractTimestamp(ClimateRecord element) {
                return element.time().toInstant().toEpochMilli();
            }
        });
    }

    private ClimateRecord toClimateRecord(ClimateData climateData) {
        ZoneId zoneId = ZoneId.of("Europe/Berlin");
        LocalDate recordingDate = climateData.getRecordingDate();
        ZonedDateTime zonedDateTime = recordingDate.atStartOfDay(zoneId);
        return new ClimateRecord(zonedDateTime, climateData);
    }

    private FileSource<String> getDataSource(String resourceName) {
        try {
            URL resource = this.getClass().getClassLoader().getResource(resourceName);
            Path filePath = new Path(resource.toURI());
            TextLineInputFormat format = new TextLineInputFormat(StandardCharsets.UTF_8.name());
            return FileSource.forRecordStreamFormat(format, filePath).build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
