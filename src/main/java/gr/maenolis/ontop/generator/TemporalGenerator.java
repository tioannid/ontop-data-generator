package gr.maenolis.ontop.generator;

import java.sql.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TemporalGenerator {

    private final RandomTimestampGenerator timestampGenerator;
    private final Consumer<PreparedStatement> randomAfterEvents;
    private final Consumer<PreparedStatement> randomBeforeEvents;

    public TemporalGenerator() {
        timestampGenerator = new RandomTimestampGenerator();

        randomAfterEvents = ps -> {
            try {
                ps.clearParameters();
                final Timestamp start = timestampGenerator.randomTimestamp();
                ps.setTimestamp(1, start);
                ps.setTimestamp(2, timestampGenerator.randomTimestampAfter(start));
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };

        randomBeforeEvents = ps -> {
            try {
                ps.clearParameters();
                final Timestamp end = timestampGenerator.randomTimestamp();
                ps.setTimestamp(1, timestampGenerator.randomTimestampBefore(end));
                ps.setTimestamp(2, end);
                ps.setString(3, null);
                ps.setTimestamp(4, timestampGenerator.randomTimestamp());
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        };
    }



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

    private void batchInsert(final int totalInserts, final Consumer<PreparedStatement> consumer, final Supplier<String> querySupplier) {
        try {
            Connection connection = getConnection();
            PreparedStatement st = connection.prepareStatement(querySupplier.get());

            for (int i = 0; i < totalInserts; i++) {
                if (i % BATCH_SIZE == 0 && i > 0) {
                    st.executeBatch();
                    connection.commit();
                }
                consumer.accept(st);
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
    private static final String DB_IP = "127.0.0.1";
    private static final String DB_PORT = "5432";
    private static final String DB_NAME = "endpoint";
    private static final String DB_USERNAME = "postgres";
    private static final String DB_PASSWORD = "postgres";
    private static final int BATCH_SIZE = 100;

    public static void main(String... args) {
        final TemporalGenerator gen = new TemporalGenerator();
        gen.batchInsert(10000, gen.randomAfterEvents, gen.insertEventsSql);
        gen.batchInsert(10000, gen.randomBeforeEvents, gen.insertEventsSql);
    }
}
