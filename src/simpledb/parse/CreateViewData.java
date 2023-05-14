package simpledb.parse;

public class CreateViewData {

    private String viewName;
    private QueryData queryData;

    public CreateViewData(String viewName, QueryData queryData) {
        this.viewName = viewName;
        this.queryData = queryData;
    }

    public String getViewName() {
        return viewName;
    }

    public String getViewDef() {
        return queryData.toString();
    }
}
