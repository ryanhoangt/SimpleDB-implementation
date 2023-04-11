package simpledb.record;

import java.util.Objects;

/**
 * An identifier for a record withinin a file, using the combination
 * of the block number in the file and the location of the record in
 * that block.
 */
public class RID {

    private int blkNum, slot;

    public RID(int blkNum, int slot) {
        this.blkNum = blkNum;
        this.slot = slot;
    }

    public int getBlkNum() {
        return blkNum;
    }

    public int getSlot() {
        return slot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RID rid = (RID) o;
        return blkNum == rid.blkNum && slot == rid.slot;
    }

    @Override
    public String toString() {
        return "RID{" +
                "blkNum=" + blkNum +
                ", slot=" + slot +
                '}';
    }
}
