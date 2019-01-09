package com.rrc.bigdata.model;

import java.text.SimpleDateFormat;

public class ClickHouseTableInfo {

    private String localTable;

    private String table;

    private String sinkDateCol;

    private String sourceDateCol;

    private SimpleDateFormat df;

    public ClickHouseTableInfo() {
    }

    public String getLocalTable() {
        return localTable;
    }

    public void setLocalTable(String localTable) {
        this.localTable = localTable;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSinkDateCol() {
        return sinkDateCol;
    }

    public void setSinkDateCol(String sinkDateCol) {
        this.sinkDateCol = sinkDateCol;
    }

    public String getSourceDateCol() {
        return sourceDateCol;
    }

    public void setSourceDateCol(String sourceDateCol) {
        this.sourceDateCol = sourceDateCol;
    }

    public SimpleDateFormat getDf() {
        return df;
    }

    public void setDf(SimpleDateFormat df) {
        this.df = df;
    }
}

