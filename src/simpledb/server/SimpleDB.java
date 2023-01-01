package simpledb.server;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;

import java.io.File;

public class SimpleDB {
    public static int BLOCK_SIZE = 400;

    private FileMgr fm;

    public SimpleDB(String dirname, int blocksize, int buffsize) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, blocksize);

    }

    public FileMgr fileMgr() {
        return fm;
    }

}
