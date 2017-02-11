package gr.maenolis.ontop.model;

import java.sql.Timestamp;

/**
 * Created by maenolis on 11/2/2017.
 */
public class Period {

    private Timestamp start;
    private Timestamp end;

    public Period(Timestamp start, Timestamp end) {
        this.start = start;
        this.end = end;
    }

    public Period() {}

    public Timestamp getStart() {
        return start;
    }

    public void setStart(Timestamp start) {
        this.start = start;
    }

    public Timestamp getEnd() {
        return end;
    }

    public void setEnd(Timestamp end) {
        this.end = end;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Period{");
        sb.append("start=").append(start);
        sb.append(", end=").append(end);
        sb.append('}');
        return sb.toString();
    }
}
