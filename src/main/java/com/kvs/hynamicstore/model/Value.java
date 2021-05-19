package com.kvs.hynamicstore.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class Value implements Serializable {
    private String val;
    private int reqCount;

    public Value(String val, int i) {
        setVal(val);
        setReqCount(i);
    }
    public String toString(){
        return val+"("+reqCount+")";
    }
}
