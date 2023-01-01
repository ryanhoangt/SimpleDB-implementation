package simpledb.file;

import java.util.Objects;

public class BlockId {
    private String filename;
    private int blknum;

    public BlockId(String filename, int blknum) {
        this.filename = filename;
        this.blknum = blknum;
    }

    public String fileName() {
        return filename;
    }

    public int number() {
        return blknum;
    }

    @Override
    public String toString() {
        return "BlockId{" +
                "filename='" + filename + '\'' +
                ", blknum=" + blknum +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlockId blockId = (BlockId) o;
        return blknum == blockId.blknum && filename.equals(blockId.filename);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
