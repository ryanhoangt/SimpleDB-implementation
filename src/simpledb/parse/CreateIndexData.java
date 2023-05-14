package simpledb.parse;

public class CreateIndexData {

    private String idxName, tblName, fldName;

    public CreateIndexData(String idxName, String tblName, String fldName) {
        this.idxName = idxName;
        this.tblName = tblName;
        this.fldName = fldName;
    }

    public String getIdxName() {
        return idxName;
    }

    public String getTblName() {
        return tblName;
    }

    public String getFldName() {
        return fldName;
    }
}
