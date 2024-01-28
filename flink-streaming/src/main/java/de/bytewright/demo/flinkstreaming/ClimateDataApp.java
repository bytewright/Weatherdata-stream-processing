package de.bytewright.demo.flinkstreaming;

import de.bytewright.demo.flinkstreaming.climatedata.ClimateStreamerExample;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.util.StopWatch;

import java.util.concurrent.TimeUnit;

@ShellComponent
@RequiredArgsConstructor
public class ClimateDataApp {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ClimateDataApp.class);
    private final ClimateStreamerExample climateStreamerExample;


    @ShellMethod(key = "climate")
    public String startClimateFLinkDemo() {
        StopWatch stopWatch = new StopWatch("climate data processing example");
        stopWatch.start("Climate data example");
        LOGGER.info("Starting climate example app...");
        climateStreamerExample.start();
        stopWatch.stop();
        return stopWatch.prettyPrint(TimeUnit.SECONDS);
    }
}
