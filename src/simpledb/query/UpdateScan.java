package simpledb.query;

import simpledb.record.RID;

/**
 * The interface implemented by all updateable scans.
 */
public interface UpdateScan extends Scan {

    public void setVal(String fieldName, Constant val);
    public void setInt(String fieldName, int val);
    public void setString(String fieldName, String val);

    /**
     * Insert a new record somewhere in the scan.
     */
    public void insert();

    /**
     * Delete the current record from the scan.
     */
    public void delete();

    /**
     * Get the id of the current record.
     * @return id of the current record.
     */
    public RID getRID();

    /**
     * Position the scan so that the current record has
     * the specified rid.
     * @param rid the id of the desired record
     */
    public void moveToRID(RID rid);
}
