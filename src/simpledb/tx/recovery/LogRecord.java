package simpledb.tx.recovery;

import simpledb.file.Page;
import simpledb.tx.Transaction;

import javax.swing.*;

public interface LogRecord {

    static final int CHECKPOINT = 0, START = 1, COMMIT = 2,
                    ROLLBACK = 3, SETINT = 4, SETSTRING = 5;

    int op();

    /**
     * Returns the transaction id stored with the log record.
     * @return the log record's transaction id
     */
    int txNumber();

    /**
     * Undoes the operation encoded by this log record.
     * The only log record types for which this method
     * does anything interesting are SETINT and SETSTRING.
     * @param tx the transaction that is performing the undo.
     */
    void undo(Transaction tx);

    static LogRecord reconstructLogRecord(byte[] bytes) {
        Page p = new Page(bytes);
        switch (p.getInt(0)) {
            case CHECKPOINT:
                return new CheckpointRecord();
            case START:
                return new StartRecord(p);
            case COMMIT:
                return new CommitRecord(p);
            case ROLLBACK:
                return new RollbackRecord(p);
            case SETINT:
                return new SetIntRecord(p);
            case SETSTRING:
                return new SetStringRecord(p);
            default:
                return null;
        }
    }

}
