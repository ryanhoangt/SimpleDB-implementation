package simpledb.metadata;

/**
 * A StatInfo object holds three pieces of statistical
 * information about a table: the number of blocks, number of records,
 * and number of distinct values for each field.
 */
public class StatInfo {
    private int numBlocks;
    private int numRecs;

    public StatInfo(int numBlocks, int numRecs) {
        this.numBlocks = numBlocks;
        this.numRecs = numRecs;
    }

    public int estimatedNumBlocks() {
        return numBlocks;
    }

    public int estimatedNumRecords() {
        return numRecs;
    }

    /**
     * Return the estimated number of distinct values for the
     * specified field.
     * For simplicity, this estimate is a random guess to be 1/3
     * the number of total records in the table.
     * @param fieldName
     * @return
     */
    public int estimatedNumDistinctValues(String fieldName) {
        return 1 + (numRecs / 3);
    }
}
