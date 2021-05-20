package com.kvs.hynamicstore.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Hashtable;

@Getter
@Setter
public class Table extends Hashtable implements Serializable {
    private String tableName;
    private Hashtable<String, Value> table;
    private int reqCount;

    public Table(String tableName, Hashtable<String, Value> hashtable, int i) {
        setTableName(tableName);
        setTable(hashtable);
        setReqCount(i);
    }

    public Table() {

    }

    public String toString(){
        return table+"("+reqCount+")";
    }
}
