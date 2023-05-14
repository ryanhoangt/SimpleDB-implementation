package simpledb.parse;

import simpledb.query.Predicate;

public class DeleteData {
    private String tblName;
    private Predicate pred;

    public DeleteData(String tblName, Predicate pred) {
        this.tblName = tblName;
        this.pred = pred;
    }

    public String getTblName() {
        return tblName;
    }

    public Predicate getPred() {
        return pred;
    }


}
