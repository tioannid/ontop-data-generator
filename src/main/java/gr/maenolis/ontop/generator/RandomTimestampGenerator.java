package gr.maenolis.ontop.generator;

import org.apache.commons.lang3.RandomUtils;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class RandomTimestampGenerator {

    // -- Data Members
    private final long calendarMSecs;

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
    }
    
    // 2. default constructor uses (30-Nov-2050)
    public RandomTimestampGenerator() {
        this(2050, 11, 30);
    }

    // -- Methods
    public final Timestamp randomTimestamp() {
        // get a random long number between 0L and
        // the generator's calendar time value expressed in msecs
        return new Timestamp(RandomUtils.nextLong(0L, calendarMSecs));
    }

    public final Timestamp randomTimestampAfter(final Date startDate) {
        // get a random long number between the number of msecs represented by
        // startDate and the generator's calendar time value expressed in msecs
        return new Timestamp(RandomUtils.nextLong(startDate.getTime(), calendarMSecs));
    }

    public final Timestamp randomTimestampBefore(final Date endDate) {
        // get a random long number between 0L and the number of msecs 
        // represented by endDate
        return new Timestamp(RandomUtils.nextLong(0L, endDate.getTime()));
    }

    public final Timestamp randomTimestampBetween(final Date start, final Date endDate) {
        // get a random long number between the number of msecs represented by
        // startDate and the number of msecs represented by endDate
        return new Timestamp(RandomUtils.nextLong(start.getTime(), endDate.getTime()));
    }
}
