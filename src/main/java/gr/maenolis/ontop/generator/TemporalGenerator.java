package gr.maenolis.ontop.generator;

import gr.maenolis.ontop.model.Period;

import java.sql.*;
import java.util.function.*;

public class TemporalGenerator {

    private final RandomTimestampGenerator timestampGenerator;
    private final Function<PreparedStatement, Period> randomPeriod;
    private final BiConsumer<PreparedStatement, Period> adjacentAfterPeriod;
    private final BiConsumer<PreparedStatement, Period> adjacentBeforePeriod;
    private final BiConsumer<PreparedStatement, Period> containsPeriod;
    private final BiConsumer<PreparedStatement, Period> containedByPeriod;
    private final BiConsumer<PreparedStatement, Period> overRightPeriod;
    private final BiConsumer<PreparedStatement, Period> overLeftPeriod;
    private final BiConsumer<PreparedStatement, Period> equalPeriod;

    public TemporalGenerator() {
        timestampGenerator = new RandomTimestampGenerator();

        randomPeriod = ps -> {
            try {
                ps.clearParameters();
                final Timestamp start = timestampGenerator.randomTimestamp();
                final Timestamp end = timestampGenerator.randomTimestampAfter(start);
                ps.setTimestamp(1, start);
                ps.setTimestamp(2, end);
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
                return new Period(start, end);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        adjacentAfterPeriod = (ps, period) -> {
            try {
                ps.clearParameters();
                ps.setTimestamp(1, period.getStart());
                final Timestamp end = timestampGenerator.randomTimestampAfter(period.getStart());
                ps.setTimestamp(2, end);
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        adjacentBeforePeriod = (ps, period) -> {
            try {
                ps.clearParameters();
                final Timestamp start = timestampGenerator.randomTimestampBefore(period.getEnd());
                ps.setTimestamp(1, start);
                ps.setTimestamp(2, period.getEnd());
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        containsPeriod = (ps, period) -> {
            try {
                ps.clearParameters();
                final Timestamp start = timestampGenerator.randomTimestampBefore(period.getStart());
                final Timestamp end = timestampGenerator.randomTimestampAfter(period.getEnd());
                ps.setTimestamp(1, start);
                ps.setTimestamp(2, end);
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        containedByPeriod = (ps, period) -> {
            try {
                ps.clearParameters();
                final Timestamp start = timestampGenerator.randomTimestampAfter(period.getStart());
                final Timestamp end = timestampGenerator.randomTimestampBefore(period.getEnd());
                ps.setTimestamp(1, start);
                ps.setTimestamp(2, end);
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        overRightPeriod = (ps, period) -> {
            try {
                ps.clearParameters();
                final Timestamp start = timestampGenerator.randomTimestampBefore(period.getEnd());
                final Timestamp end = timestampGenerator.randomTimestampAfter(period.getEnd());
                ps.setTimestamp(1, start);
                ps.setTimestamp(2, end);
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        overLeftPeriod = (ps, period) -> {
            try {
                ps.clearParameters();
                final Timestamp start = timestampGenerator.randomTimestampBefore(period.getStart());
                final Timestamp end = timestampGenerator.randomTimestampAfter(period.getStart());
                ps.setTimestamp(1, start);
                ps.setTimestamp(2, end);
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        equalPeriod = (ps, period) -> {
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
    }


    /**
     * TODO:
     */
    private final Supplier<String> insertMeetingsSql = () -> {
        final String insertQuery = "INSERT INTO meeting(\n" +
                "            id, name, location, duration, creation_date)\n" +
                "    VALUES (?, ?, ?, ?, ?);";
        return insertQuery;
    };

    private final Supplier<String> insertEventsSql = () -> {
        final String insertQuery = "INSERT INTO event(\n" +
                "            id, duration, description, time_propagated)\n" +
                "    VALUES (nextval('event_id_seq'), period_oc(?, ?), ?, ?);";
        return insertQuery;
    };

    private void batchInsert(final int totalInserts, final Function<PreparedStatement, Period> function, final BiConsumer<PreparedStatement, Period> consumer, final Supplier<String> querySupplier) {
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
    private static final String DB_IP = "192.168.1.66";
    private static final String DB_PORT = "5432";
    private static final String DB_NAME = "endpoint";
    private static final String DB_USERNAME = "postgres";
    private static final String DB_PASSWORD = "postgres";
    private static final int BATCH_SIZE = 100;

    public static void main(String... args) {
        final TemporalGenerator gen = new TemporalGenerator();
        /**
         * Events inserting.
         */
        gen.batchInsert(1000000, gen.randomPeriod, gen.adjacentAfterPeriod, gen.insertEventsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.adjacentBeforePeriod, gen.insertEventsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.containsPeriod, gen.insertEventsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.containedByPeriod, gen.insertEventsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.overRightPeriod, gen.insertEventsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.overLeftPeriod, gen.insertEventsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.equalPeriod, gen.insertEventsSql);

        /**
         * Meetings inserting.
         */
        gen.batchInsert(1000000, gen.randomPeriod, gen.adjacentAfterPeriod, gen.insertMeetingsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.adjacentBeforePeriod, gen.insertMeetingsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.containsPeriod, gen.insertMeetingsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.containedByPeriod, gen.insertMeetingsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.overRightPeriod, gen.insertMeetingsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.overLeftPeriod, gen.insertMeetingsSql);
        gen.batchInsert(1000000, gen.randomPeriod, gen.equalPeriod, gen.insertMeetingsSql);
    }
}
