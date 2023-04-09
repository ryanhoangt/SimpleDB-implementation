package simpledb.buffer;

/**
 * A runtime exception indicating that the transaction
 * needs to abort since no buffer is available to use.
 * Client is responsible for handling it.
 */
public class NoBufferAvailableException extends RuntimeException {
    public NoBufferAvailableException() { }
}
