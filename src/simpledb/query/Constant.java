package simpledb.query;

/**
 * The class that denotes values stored in the database.
 */
public class Constant implements Comparable<Constant> {

    private Integer iVal = null;
    private String sVal = null;

    public Constant(Integer iVal) {
        this.iVal = iVal;
    }

    public Constant(String sVal) {
        this.sVal = sVal;
    }

    public int asInt() {
        return iVal;
    }

    public String asString() {
        return sVal;
    }

    @Override
    public int compareTo(Constant o) {
        return (iVal != null) ? iVal.compareTo(o.iVal)
                : sVal.compareTo(o.sVal);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Constant constant = (Constant) o;
        return (iVal != null) ? iVal.equals(constant.iVal)
                : sVal.equals(constant.sVal);
    }

    @Override
    public int hashCode() {
        return (iVal != null) ? iVal.hashCode() : sVal.hashCode();
    }

    @Override
    public String toString() {
        return (iVal != null) ? iVal.toString() : sVal.toString();
    }
}
