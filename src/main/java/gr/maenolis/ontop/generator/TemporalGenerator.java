package gr.maenolis.ontop.generator;

import gr.maenolis.ontop.model.Period;

import java.sql.*;
import java.util.function.*;

public class TemporalGenerator {

    // -- Static Members
    private final static String insertMeetingsSql = "INSERT INTO meeting(\n"
            + "            id, name, duration, creation_date, location)\n"
            + "    VALUES (nextval('meeting_id_seq'), ?, period_oc(?, ?), ?, ?);";
    private final static String insertEventsSql = "INSERT INTO event(\n"
            + "            id, duration, description, time_propagated)\n"
            + "    VALUES (nextval('event_id_seq'), period_oc(?, ?), ?, ?);";
    private static final String DRIVER_NAME = "org.postgresql.Driver";
    private static final String URL_PREFIX = "jdbc:postgresql://";
    private static final String DB_IP = "localhost";
    private static final String DB_PORT = "5432";
    private static final String DB_NAME = "endpoint";
    private static final String DB_USERNAME = "postgres";
    private static final String DB_PASSWORD = "postgres";
    private static final int BATCH_SIZE = 100;
    private static final int INSERTS_PER_CATEGORY = 40000;
    private static final int YEAR = 2050;
    private static final int MONTH = 11;
    private static final int DAY = 30;

    // -- Data Members
    private String dbName;
    private int batchSize;
    private int insertsPerCategory;

    private final RandomTimestampGenerator timestampGenerator;
    private final Supplier<Period> randomPeriod;
    private final Function<Period, Period> adjacentAfterPeriod;
    private final Function<Period, Period> adjacentBeforePeriod;
    private final Function<Period, Period> containsPeriod;
    private final Function<Period, Period> containedByPeriod;
    private final Function<Period, Period> overRightPeriod;
    private final Function<Period, Period> overLeftPeriod;
    private final Function<Period, Period> equalPeriod;
    private final BiConsumer<PreparedStatement, Period> insertEvent;
    private final BiConsumer<PreparedStatement, Period> insertMeeting;

    // -- Constructors
    // 1. constructor which allows to connect to a custom named db with
    //    the desired number of insertions/category and desired batch size
    //    for batch updates and custome maximum calendar timestamp
    public TemporalGenerator(String dbName, int batchSize, int insertsPerCategory, int year, int month, int day) {
        this.dbName = dbName;
        this.batchSize = batchSize;
        this.insertsPerCategory = insertsPerCategory;
        timestampGenerator = new RandomTimestampGenerator(year, month, day);

        // returns a Period with start timestamp <= timestampGenerator base date
        // (30-Dec-2050) and end timestamp, start <= end <=  (30-Dec-2050)
        randomPeriod = new Supplier<Period>() {
            @Override
            public Period get() {
                final Timestamp start = timestampGenerator.randomTimestamp();
                return new Period(start, timestampGenerator.randomTimestampAfter(start));
            }
        };

        // returns a Period adjacent AFTER the input period
        // input period  : [ start, end ]
        // output period :        [ start, end ) 
        adjacentAfterPeriod = new Function<Period, Period>() {
            @Override
            public Period apply(Period t) {
                return new Period(t.getEnd(), timestampGenerator.randomTimestampAfter(t.getEnd()));
            }
        };

        // returns a Period adjacent BEFORE the input period
        // input period  :        [ start, end ]
        // output period : [ start, end ] 
        adjacentBeforePeriod = new Function<Period, Period>() {
            @Override
            public Period apply(Period t) {
                return new Period(timestampGenerator.randomTimestampBefore(t.getStart()), t.getStart());
            }
        };

        // returns a Period that CONTAINS the input period
        // input period  :   [ start,  end ]
        // output period : [ start,      end ] 
        containsPeriod = new Function<Period, Period>() {
            @Override
            public Period apply(Period t) {
                return new Period(timestampGenerator.randomTimestampBefore(t.getStart()), timestampGenerator.randomTimestampAfter(t.getEnd()));
            }
        };

        // returns a Period that is CONTAINED_BY the input period
        // input period  : [ start,      end ]
        // output period :   [ start,  end ] 
        containedByPeriod = new Function<Period, Period>() {
            @Override
            public Period apply(Period t) {
                Timestamp start = timestampGenerator.randomTimestampBetween(t.getStart(), t.getEnd());
                return new Period(start, timestampGenerator.randomTimestampBetween(start, t.getEnd()));
            }
        };

        // returns a Period that is OVER_RIGHT the input period
        // input period  : [ start, end ]
        // output period :              [ start,  end ] 
        overRightPeriod = new Function<Period, Period>() {
            @Override
            public Period apply(Period t) {
                Timestamp start = timestampGenerator.randomTimestampAfter(t.getEnd());
                return new Period(start, timestampGenerator.randomTimestampAfter(start));
            }
        };

        // returns a Period that is OVER_LEFT the input period
        // input period  :               [ start, end ]
        // output period : [ start,  end ] 
        overLeftPeriod = new Function<Period, Period>() {
            @Override
            public Period apply(Period t) {
                Timestamp end = timestampGenerator.randomTimestampBefore(t.getStart());
                return new Period(timestampGenerator.randomTimestampBefore(end), end);
            }
        };

        // returns a Period that is EQUAL with the input period
        // input period  : [ start, end ]
        // output period : [ start, end ] 
        equalPeriod = new Function<Period, Period>() {
            @Override
            public Period apply(Period t) {
                return new Period(t);
            }
        };

        insertEvent = new BiConsumer<PreparedStatement, Period>() {
            @Override
            public void accept(PreparedStatement ps, Period p) {
                try {
                    // id, duration, description, time_propagated
                    ps.clearParameters();
                    ps.setTimestamp(1, p.getStart());
                    ps.setTimestamp(2, p.getEnd());
                    ps.setString(3, null); 
                    ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                    ps.addBatch();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        insertMeeting = new BiConsumer<PreparedStatement, Period>() {
            @Override
            public void accept(PreparedStatement ps, Period p) {
                try {
                    ps.clearParameters();
                    ps.setString(1, null); // name 
                    ps.setTimestamp(2, p.getStart());
                    ps.setTimestamp(3, p.getEnd());
                    ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
                    ps.setString(5, null); // location
                    ps.addBatch();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    // 2. default constructor uses static default values 
    public TemporalGenerator() {
        this(TemporalGenerator.DB_NAME,
                TemporalGenerator.BATCH_SIZE,
                TemporalGenerator.INSERTS_PER_CATEGORY,
                TemporalGenerator.YEAR,
                TemporalGenerator.MONTH,
                TemporalGenerator.DAY);
    }

    private void batchOperation(final Supplier<Period> starter,
            final Function<Period, Period> function, final BiConsumer<PreparedStatement, Period> biConsumer, String queryStr) {
        try {
            Connection connection = getConnection(this.dbName);
            PreparedStatement st = connection.prepareStatement(queryStr);

            final Period random = starter.get();

            for (int i = 0; i < this.insertsPerCategory; i++) {
                if (i % this.batchSize == 0 && i > 0) {
                    st.executeBatch();
                    connection.commit();
                }
                final Period current = function.apply(random);
                biConsumer.accept(st, current);
            }
            st.executeBatch();
            connection.commit();

            st.close();
            closeConnection(connection);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static Connection getConnection(String dbName) throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER_NAME);
        Connection connection = DriverManager.getConnection(URL_PREFIX + DB_IP + ":" + DB_PORT + "/" + dbName, DB_USERNAME, DB_PASSWORD);
        connection.setAutoCommit(false);
        return connection;
    }

    private static void closeConnection(Connection connection) throws SQLException {
        connection.close();
    }

    public static void main(String... args) {
//        final TemporalGenerator gen = new TemporalGenerator(TemporalGenerator.DB_NAME,
//                100,
//                1000,
//                2022,
//                5,
//                21);

        final TemporalGenerator gen = new TemporalGenerator();
        /**
         * Events inserting.
         */
        gen.batchOperation(gen.randomPeriod, gen.adjacentAfterPeriod, gen.insertEvent, TemporalGenerator.insertEventsSql);
        gen.batchOperation(gen.randomPeriod, gen.adjacentBeforePeriod, gen.insertEvent, TemporalGenerator.insertEventsSql);
        gen.batchOperation(gen.randomPeriod, gen.containsPeriod, gen.insertEvent, TemporalGenerator.insertEventsSql);
        gen.batchOperation(gen.randomPeriod, gen.containedByPeriod, gen.insertEvent, TemporalGenerator.insertEventsSql);
        gen.batchOperation(gen.randomPeriod, gen.overRightPeriod, gen.insertEvent, TemporalGenerator.insertEventsSql);
        gen.batchOperation(gen.randomPeriod, gen.overLeftPeriod, gen.insertEvent, TemporalGenerator.insertEventsSql);
        gen.batchOperation(gen.randomPeriod, gen.equalPeriod, gen.insertEvent, TemporalGenerator.insertEventsSql);

        /**
         * Meetings inserting.
         */
        gen.batchOperation(gen.randomPeriod, gen.adjacentAfterPeriod, gen.insertMeeting, TemporalGenerator.insertMeetingsSql);
        gen.batchOperation(gen.randomPeriod, gen.adjacentBeforePeriod, gen.insertMeeting, TemporalGenerator.insertMeetingsSql);
        gen.batchOperation(gen.randomPeriod, gen.containsPeriod, gen.insertMeeting, TemporalGenerator.insertMeetingsSql);
        gen.batchOperation(gen.randomPeriod, gen.containedByPeriod, gen.insertMeeting, TemporalGenerator.insertMeetingsSql);
        gen.batchOperation(gen.randomPeriod, gen.overRightPeriod, gen.insertMeeting, TemporalGenerator.insertMeetingsSql);
        gen.batchOperation(gen.randomPeriod, gen.overLeftPeriod, gen.insertMeeting, TemporalGenerator.insertMeetingsSql);
        gen.batchOperation(gen.randomPeriod, gen.equalPeriod, gen.insertMeeting, TemporalGenerator.insertMeetingsSql);
    }
}
