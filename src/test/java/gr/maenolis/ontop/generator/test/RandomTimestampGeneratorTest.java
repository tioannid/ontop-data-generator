package gr.maenolis.ontop.generator.test;

import gr.maenolis.ontop.generator.RandomTimestampGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

public class RandomTimestampGeneratorTest {

    private static final int NUMBER_OF_TESTS = 1000;

    @Test
    public void timestampBefore() {
        final RandomTimestampGenerator generator = new RandomTimestampGenerator();
        Timestamp t1,t2;
        for (int i = 0; i < NUMBER_OF_TESTS; i++) {
            t1 = generator.randomTimestamp();
            t2 = generator.randomTimestampBefore(t1);
            Assert.assertTrue(t2.before(t1));
        }
    }

    @Test
    public void timestampAfter() {
        final RandomTimestampGenerator generator = new RandomTimestampGenerator();
        Timestamp t1,t2;
        for (int i = 0; i < NUMBER_OF_TESTS; i++) {
            t1 = generator.randomTimestamp();
            t2 = generator.randomTimestampAfter(t1);
            Assert.assertTrue(t2.after(t1));
        }
    }
}
