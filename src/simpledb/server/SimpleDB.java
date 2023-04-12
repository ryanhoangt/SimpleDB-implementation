package simpledb.server;

import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.metadata.MetaDataMgr;
import simpledb.tx.Transaction;

import java.io.File;

public class SimpleDB {
    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;
    public static String LOG_FILE = "simpledb.log";

    private FileMgr fm;
    private LogMgr lm;
    private BufferMgr bm;
    private MetaDataMgr mdm;

    /**
     * The constructor for most situations. Unlike the debugging constructor,
     * it also initializes the metadata tables.
     * @param dirname the name of the database directory
     */
    public SimpleDB(String dirname) {
        this(dirname, BLOCK_SIZE, BUFFER_SIZE);
        Transaction tx = newTx();
        boolean isNew = fm.isNew();
        if (isNew)
            System.out.println("Creating new database...");
        else {
            System.out.println("Recovering existing database...");
            tx.recover();
        }
        mdm = new MetaDataMgr(isNew, tx);
        tx.commit();
    }

    /**
     * A constructor useful for debugging.
     */
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
