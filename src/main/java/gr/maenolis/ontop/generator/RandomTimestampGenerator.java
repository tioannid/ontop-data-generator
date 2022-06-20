package gr.maenolis.ontop.generator;

import org.apache.commons.lang3.RandomUtils;

import java.sql.Timestamp;
import java.util.Calendar;

public class RandomTimestampGenerator {

    // -- Static Members
    final Timestamp zeroDayTS = new Timestamp(0L);

    // -- Data Members
    private final long calendarMSecs; // max timestamp in the domain in msecs
    private final Timestamp calRefTS; // max timestamp in the domain
    // therefore our time domain is bound to:
    // [zeroDayTS, calRefTS]
    
    // -- Constructors
    // 1. constructor allows to use as reference a date of choice
    public RandomTimestampGenerator(int year, int month, int day) {
        // get a Calendar object whose calendar fields have been initialized 
        // with the current date and time
        Calendar end = Calendar.getInstance();
        // set the values for the calendar fields (YEAR, MONTH, DAY_OF_MONTH) 
        // to (2050, 11,30). that is, 30th of November 2050
        end.set(year, month, day);
        calendarMSecs = end.getTimeInMillis();
        calRefTS = new Timestamp(calendarMSecs);
    }

    // 2. default constructor uses (30-Nov-2050)
    public RandomTimestampGenerator() {
        this(2050, 11, 30);
    }

    // -- Data Accessors
    public long getCalendarMSecs() {
        return calendarMSecs;
    }

    // -- Methods
    public final Timestamp randomTimestampBetween(final Timestamp start, final Timestamp end) {
        // get a timestamp between start and end timestamps
        return new Timestamp(RandomUtils.nextLong(start.getTime(), end.getTime()));
    }

    public final Timestamp randomTimestampBefore(final Timestamp end) {
        // get a timestamp between zeroDayTS and end
        return randomTimestampBetween(zeroDayTS, end);
    }

    public final Timestamp randomTimestampAfter(final Timestamp start) {
        // get a random long number between the number of msecs represented by
        // start and the generator's calendar time value expressed in msecs
        return randomTimestampBetween(start, this.calRefTS);
    }

    public final Timestamp randomTimestamp() {
        // get a timestamp between zeroDayTS and calRefTS
        return randomTimestampBefore(calRefTS);
    }
}
