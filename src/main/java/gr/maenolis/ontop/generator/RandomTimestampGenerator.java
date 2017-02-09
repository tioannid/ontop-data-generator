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

    private static final Range<Integer> YEARS = Range.between(1900, 2016);
    private static final Range<Integer> MONTHS = Range.between(0, 11);
    private static final Range<Integer> DAYS = Range.between(1, 28);
    private static final Range<Integer> HOURS = Range.between(0, 23);
    private static final Range<Integer> MINUTES = Range.between(0, 59);
    private static final Range<Integer> SECONDS = Range.between(0, 59);

    private final ThreadLocal<Calendar> cal = ThreadLocal.withInitial(() -> Calendar.getInstance());

    private ThreadLocal<Supplier<Calendar>> calendarSupplier = new ThreadLocal<>();
    private final Predicate<Calendar> isAfter = after -> cal.get().after(calendarSupplier.get().get());

    public final Timestamp randomTimestamp() {

        final Calendar calendar = cal.get();
        calendar.set(
                RandomUtils.nextInt(YEARS.getMinimum(), YEARS.getMaximum()),
                RandomUtils.nextInt(MONTHS.getMinimum(), MONTHS.getMaximum()),
                RandomUtils.nextInt(DAYS.getMinimum(), DAYS.getMaximum()),
                RandomUtils.nextInt(HOURS.getMinimum(), HOURS.getMaximum()),
                RandomUtils.nextInt(MINUTES.getMinimum(), MINUTES.getMaximum()),
                RandomUtils.nextInt(SECONDS.getMinimum(), SECONDS.getMaximum())
        );

        return new Timestamp(calendar.getTimeInMillis());
    }

    public final Timestamp randomTimestampAfter(final Timestamp start) {

        final Consumer<Calendar> consumer = st -> {
            cal.get().set(
                    RandomUtils.nextInt(st.get(Calendar.YEAR), YEARS.getMaximum()),
                    RandomUtils.nextInt(st.get(Calendar.MONTH), MONTHS.getMaximum()),
                    RandomUtils.nextInt(st.get(Calendar.DAY_OF_MONTH), DAYS.getMaximum()),
                    RandomUtils.nextInt(st.get(Calendar.HOUR_OF_DAY), HOURS.getMaximum()),
                    RandomUtils.nextInt(st.get(Calendar.MINUTE), MINUTES.getMaximum()),
                    RandomUtils.nextInt(st.get(Calendar.SECOND), SECONDS.getMaximum())
            );
        };

        final Supplier<Calendar> supplier = () -> {
            return DateUtils.toCalendar(start);
        };

        return randomTimestamp(start, consumer, supplier, isAfter.negate());
    }

    public final Timestamp randomTimestampBefore(final Timestamp end) {

        final Consumer<Calendar> consumer = st -> {
            cal.get().set(
                    RandomUtils.nextInt(YEARS.getMinimum(), st.get(Calendar.YEAR)),
                    RandomUtils.nextInt(MONTHS.getMinimum(), st.get(Calendar.MONTH)),
                    RandomUtils.nextInt(DAYS.getMinimum(), st.get(Calendar.DAY_OF_MONTH)),
                    RandomUtils.nextInt(HOURS.getMinimum(), st.get(Calendar.HOUR_OF_DAY)),
                    RandomUtils.nextInt(MINUTES.getMinimum(), st.get(Calendar.MINUTE)),
                    RandomUtils.nextInt(SECONDS.getMinimum(), st.get(Calendar.SECOND))
            );
        };

        final Supplier<Calendar> supplier = () -> {
            return DateUtils.toCalendar(end);
        };

        return randomTimestamp(end, consumer, supplier, isAfter);
    }

    public final Timestamp randomTimestamp(final Timestamp time, final Consumer<Calendar> consumer,
           final Supplier<Calendar> supplier, final Predicate<Calendar> predicate) {

        final Calendar timeCal = DateUtils.toCalendar(time);

        calendarSupplier.set(supplier);

        int i = 0;
        do {
            consumer.accept(timeCal);
        } while (predicate.test(cal.get()));

        return new Timestamp(cal.get().getTimeInMillis());
    }
}
