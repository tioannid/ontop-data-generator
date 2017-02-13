package gr.maenolis.ontop.generator;

import gr.maenolis.ontop.model.Period;

import java.sql.*;
import java.util.function.*;

public class TemporalGenerator {

    private final RandomTimestampGenerator timestampGenerator;
    private final Supplier<Period> randomPeriod;
    private final BiConsumer<PreparedStatement, Period> insertEvent;
    private final BiConsumer<PreparedStatement, Period> insertMeeting;
    private final Function<Period, Period> adjacentAfterPeriod;
    private final Function<Period, Period> adjacentBeforePeriod;
    private final Function<Period, Period> containsPeriod;
    private final Function<Period, Period> containedByPeriod;
    private final Function<Period, Period> overRightPeriod;
    private final Function<Period, Period> overLeftPeriod;
    private final Function<Period, Period> equalPeriod;

    public TemporalGenerator() {
        timestampGenerator = new RandomTimestampGenerator();

        randomPeriod = () -> {
            final Timestamp start = timestampGenerator.randomTimestamp();
            final Timestamp end = timestampGenerator.randomTimestampAfter(start);
            return new Period(start, end);
        };

        adjacentAfterPeriod = period -> {
            final Timestamp end = timestampGenerator.randomTimestampAfter(period.getStart());
            return new Period(period.getStart(), end);
        };

        adjacentBeforePeriod = period -> {
            final Timestamp start = timestampGenerator.randomTimestampBefore(period.getEnd());
            return new Period(start, period.getEnd());
        };

        containsPeriod = period -> {
            final Timestamp start = timestampGenerator.randomTimestampBefore(period.getStart());
            final Timestamp end = timestampGenerator.randomTimestampAfter(period.getEnd());
            return new Period(start, end);
        };

        containedByPeriod = period -> {
            final Timestamp start = timestampGenerator.randomTimestampBetween(period.getStart(), period.getEnd());
            final Timestamp end = timestampGenerator.randomTimestampBetween(start, period.getEnd());
            return new Period(start, end);
        };

        overRightPeriod = period -> {
            final Timestamp start = timestampGenerator.randomTimestampBefore(period.getEnd());
            final Timestamp end = timestampGenerator.randomTimestampAfter(period.getEnd());
            return new Period(start, end);
        };

        overLeftPeriod = period -> {
            final Timestamp start = timestampGenerator.randomTimestampBefore(period.getStart());
            final Timestamp end = timestampGenerator.randomTimestampAfter(period.getStart());
            return new Period(start, end);
        };

        equalPeriod = period -> {
            return new Period(period.getStart(), period.getEnd());
        };

        insertEvent = (ps, period) -> {
            try {
                ps.clearParameters();
                ps.setTimestamp(1, period.getStart());
                ps.setTimestamp(2, period.getEnd());
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        insertMeeting = (ps, period) -> {
            try {
                ps.clearParameters();
                ps.setString(1, null);
                ps.setTimestamp(2, period.getStart());
                ps.setTimestamp(3, period.getEnd());
                ps.addBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }


    /**
     * TODO:
     */
    private final Supplier<String> insertMeetingsSql = () -> {
        final String insertQuery = "INSERT INTO meeting(\n" +
                "            id, name, duration)\n" +
                "    VALUES (nextval('meeting_id_seq'), ?, period_oc(?, ?));";
        return insertQuery;
    };

    private final Supplier<String> insertEventsSql = () -> {
        final String insertQuery = "INSERT INTO event(\n" +
                "            id, duration, description, time_propagated)\n" +
                "    VALUES (nextval('event_id_seq'), period_oc(?, ?), ?, ?);";
        return insertQuery;
    };

    private void batchInsert(final int totalInserts, final Function<PreparedStatement, Period> function,
                             final BiConsumer<PreparedStatement, Period> consumer, final Supplier<String> querySupplier) {
        try {
            Connection connection = getConnection();
            PreparedStatement st = connection.prepareStatement(querySupplier.get());

            final Period random = function.apply(st);

            for (int i = 0; i < totalInserts - 1; i++) {
                if (i % BATCH_SIZE == 0 && i > 0) {
                    st.executeBatch();
                    connection.commit();
                }
                consumer.accept(st, random);
            }
            st.executeBatch();
            connection.commit();

            st.close();
            closeConnection(connection);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void batchOperation(final int totalInserts, final Supplier<Period> starter,
                             final Function<Period, Period> function, final BiConsumer<PreparedStatement, Period> biConsumer, final Supplier<String> querySupplier) {
        try {
            Connection connection = getConnection();
            PreparedStatement st = connection.prepareStatement(querySupplier.get());

            final Period random = starter.get();

            for (int i = 0; i < totalInserts; i++) {
                if (i % BATCH_SIZE == 0 && i > 0) {
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

    private static Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER_NAME);
        Connection connection = DriverManager.getConnection(URL_PREFIX + DB_IP + ":" + DB_PORT + "/" + DB_NAME, DB_USERNAME, DB_PASSWORD);
        connection.setAutoCommit(false);
        return connection;
    }

    private static void closeConnection(Connection connection) throws SQLException {
        connection.close();
    }

    private static final String DRIVER_NAME = "org.postgresql.Driver";
    private static final String URL_PREFIX = "jdbc:postgresql://";
    private static final String DB_IP = "localhost";
    private static final String DB_PORT = "5432";
    private static final String DB_NAME = "endpoint";
    private static final String DB_USERNAME = "postgres";
    private static final String DB_PASSWORD = "postgres";
    private static final int BATCH_SIZE = 100;
    private static final int INSERT_PER_CATEGORY = 40000;

    public static void main(String... args) {
        final TemporalGenerator gen = new TemporalGenerator();
        /**
         * Events inserting.
         */
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.adjacentAfterPeriod, gen.insertEvent, gen.insertEventsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.adjacentBeforePeriod, gen.insertEvent, gen.insertEventsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.containsPeriod, gen.insertEvent, gen.insertEventsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.containedByPeriod, gen.insertEvent, gen.insertEventsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.overRightPeriod, gen.insertEvent, gen.insertEventsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.overLeftPeriod, gen.insertEvent, gen.insertEventsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.equalPeriod, gen.insertEvent, gen.insertEventsSql);

        /**
         * Meetings inserting.
         */
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.adjacentAfterPeriod, gen.insertMeeting, gen.insertMeetingsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.adjacentBeforePeriod, gen.insertMeeting, gen.insertMeetingsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.containsPeriod, gen.insertMeeting, gen.insertMeetingsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.containedByPeriod, gen.insertMeeting, gen.insertMeetingsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.overRightPeriod, gen.insertMeeting, gen.insertMeetingsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.overLeftPeriod, gen.insertMeeting, gen.insertMeetingsSql);
        gen.batchOperation(INSERT_PER_CATEGORY, gen.randomPeriod, gen.equalPeriod, gen.insertMeeting, gen.insertMeetingsSql);
    }
}
