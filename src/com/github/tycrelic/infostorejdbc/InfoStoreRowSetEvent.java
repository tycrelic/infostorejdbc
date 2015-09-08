package com.github.tycrelic.infostorejdbc;

import javax.sql.RowSet;
import javax.sql.RowSetEvent;

public class InfoStoreRowSetEvent extends RowSetEvent {

    public InfoStoreRowSetEvent(RowSet source) {
        super(source);
    }

}
