package gr.maenolis.ontop.generator.test;

import gr.maenolis.ontop.generator.RandomTimestampGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.function.Predicate;

/**
 * Created by maenolis on 11/2/2017.
 */
public class TimestampGeneratorTest {

    @Test
    public void timestampBefore() {
        final RandomTimestampGenerator generator = new RandomTimestampGenerator();
        for (int i = 0; i < 50; i++) {
            final Timestamp t1 = generator.randomTimestamp();
            final Timestamp t2 = generator.randomTimestampBefore(t1);
            Assert.assertTrue(t2.before(t1));
        }
    }

    @Test
    public void timestampAfter() {
        final RandomTimestampGenerator generator = new RandomTimestampGenerator();
        for (int i = 0; i < 50; i++) {
            final Timestamp t1 = generator.randomTimestamp();
            final Timestamp t2 = generator.randomTimestampAfter(t1);
            Assert.assertTrue(t2.after(t1));
        }
    }
}
