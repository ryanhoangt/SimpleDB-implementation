package simpledb.parse;

import simpledb.query.Constant;

import java.util.List;

public class InsertData {

    private String tblName;
    private List<String> flds;
    private List<Constant> vals;

    public InsertData(String tblName, List<String> flds, List<Constant> vals) {
        this.tblName = tblName;
        this.flds = flds;
        this.vals = vals;
    }

    public String getTblName() {
        return tblName;
    }

    public List<String> getFlds() {
        return flds;
    }

    public List<Constant> getVals() {
        return vals;
    }
}
