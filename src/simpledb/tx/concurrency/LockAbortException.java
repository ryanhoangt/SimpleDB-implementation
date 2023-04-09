package simpledb.tx.concurrency;

/**
 * A runtime exception indicating that the transaction
 * needs to abort since a lock could not be obtained.
 * Client is responsible for handling it.
 */
public class LockAbortException extends RuntimeException {

    public LockAbortException() { }

}
