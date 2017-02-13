package gr.maenolis.ontop.generator;

import org.apache.commons.lang3.RandomUtils;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class RandomTimestampGenerator {

    private final Calendar end;

    public RandomTimestampGenerator() {
        end = Calendar.getInstance();
        end.set(2050, 11,30);
    }

    public final Timestamp randomTimestamp() {
        return new Timestamp(RandomUtils.nextLong(0L, end.getTimeInMillis()));
    }

    public final Timestamp randomTimestampAfter(final Date start) {
        return new Timestamp(RandomUtils.nextLong(start.getTime(), end.getTimeInMillis()));
    }

    public final Timestamp randomTimestampBefore(final Date end) {
        return new Timestamp(RandomUtils.nextLong(0L, end.getTime()));
    }

    public final Timestamp randomTimestampBetween(final Date start, final Date end) {
        return new Timestamp(RandomUtils.nextLong(start.getTime(), end.getTime()));
    }
}
