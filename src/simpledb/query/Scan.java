package simpledb.query;

/**
 * This interface will be implemented by each query scan.
 * There is a Scan class for each Relational algebra operator.
 */
public interface Scan {

    /**
     * Position the scan before its first record. A subsequent call
     * to next() will return the first record.
     */
    public void beforeFirst();

    /**
     * Move the scan to the next record.
     * @return false if there is no next record, true otherwise.
     */
    public boolean next();

    public int getInt(String fieldName);
    public String getString(String fieldName);
    public Constant getVal(String fieldName);
    public boolean hasField(String fieldName);

    /**
     * Close the scan and its subscans, if having any.
     */
    public void close();

}
