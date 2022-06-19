package gr.maenolis.ontop.model;

import java.sql.Timestamp;

/**
 * Created by maenolis on 11/2/2017.
 */
public class Period {

    // -- Data Members    
    private Timestamp start;
    private Timestamp end;

    // -- Constructors
    // 1. constructor allows to define a period with start and end timestamps
    public Period(Timestamp start, Timestamp end) {
        this.start = start;
        this.end = end;
    }

    // -- Data Accessors
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

    // -- Methods
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(this.getClass().getSimpleName());
        sb.append("{");
        sb.append("start=").append(start);
        sb.append(", end=").append(end);
        sb.append('}');
        return sb.toString();
    }
}