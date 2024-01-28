package de.bytewright.demo.flinkstreaming.climatedata.dto;

import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.StringJoiner;

public final class WeekClimateRecord {
    private final ZonedDateTime minTime;
    private final ZonedDateTime maxTime;
    private final double tempSum;
    private final int dataCount;

    public WeekClimateRecord(ZonedDateTime minTime, ZonedDateTime maxTime, double tempSum, int dataCount) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.tempSum = tempSum;
        this.dataCount = dataCount;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", WeekClimateRecord.class.getSimpleName() + "[", "]")
                .add("minTime=" + minTime)
                .add("maxTime=" + maxTime)
                .add("tempSum=" + tempSum)
                .add("dataCount=" + dataCount)
                .toString();
    }

    public ZonedDateTime minTime() {
        return minTime;
    }

    public ZonedDateTime maxTime() {
        return maxTime;
    }

    public double tempSum() {
        return tempSum;
    }

    public int dataCount() {
        return dataCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (WeekClimateRecord) obj;
        return Objects.equals(this.minTime, that.minTime) &&
                Objects.equals(this.maxTime, that.maxTime) &&
                Double.doubleToLongBits(this.tempSum) == Double.doubleToLongBits(that.tempSum) &&
                this.dataCount == that.dataCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minTime, maxTime, tempSum, dataCount);
    }

}
