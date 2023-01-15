package simpledb.server;

import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

import java.io.File;

public class SimpleDB {
    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;
    public static String LOG_FILE = "simpledb.log";

    private FileMgr fm;
    private LogMgr lm;
    private BufferMgr bm;

    public SimpleDB(String dirname, int blocksize, int buffsize) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, blocksize);
        lm = new LogMgr(fm, LOG_FILE);
        bm = new BufferMgr(fm, lm, buffsize);
    }

    /**
     * A convenient way for clients to create transactions
     */
    public Transaction newTx() {
        return new Transaction(fm, lm, bm);
    }

    public FileMgr fileMgr() {
        return fm;
    }

    public LogMgr logMgr() {
        return lm;
    }

    public BufferMgr bufferMgr() {
        return bm;
    }
}
