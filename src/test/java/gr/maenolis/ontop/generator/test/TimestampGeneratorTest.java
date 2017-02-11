package gr.maenolis.ontop.generator.test;

import gr.maenolis.ontop.generator.RandomTimestampGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

/**
 * Created by maenolis on 11/2/2017.
 */
public class TimestampGeneratorTest {

    private static final int NUMBER_OF_TESTS = 1000;

    @Test
    public void timestampBefore() {
        final RandomTimestampGenerator generator = new RandomTimestampGenerator();
        for (int i = 0; i < NUMBER_OF_TESTS; i++) {
            final Timestamp t1 = generator.randomTimestamp();
            final Timestamp t2 = generator.randomTimestampBefore(t1);
            Assert.assertTrue(t2.before(t1));
        }
    }

    @Test
    public void timestampAfter() {
        final RandomTimestampGenerator generator = new RandomTimestampGenerator();
        for (int i = 0; i < NUMBER_OF_TESTS; i++) {
            final Timestamp t1 = generator.randomTimestamp();
            final Timestamp t2 = generator.randomTimestampAfter(t1);
            Assert.assertTrue(t2.after(t1));
        }
    }
}
