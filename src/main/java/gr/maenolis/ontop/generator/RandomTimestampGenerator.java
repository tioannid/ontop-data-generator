package gr.maenolis.ontop.generator;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.time.DateUtils;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class RandomTimestampGenerator {

    public RandomTimestampGenerator() {
    }

    public final Timestamp randomTimestamp() {
        return new Timestamp(RandomUtils.nextLong(0L, Long.MAX_VALUE));
    }

    public final Timestamp randomTimestampAfter(final Timestamp start) {
        return new Timestamp(RandomUtils.nextLong(start.getTime(), Long.MAX_VALUE));
    }

    public final Timestamp randomTimestampBefore(final Timestamp end) {
        return new Timestamp(RandomUtils.nextLong(0L, end.getTime()));
    }
}
