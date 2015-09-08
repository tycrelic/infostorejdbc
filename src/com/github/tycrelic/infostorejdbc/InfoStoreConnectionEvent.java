package com.github.tycrelic.infostorejdbc;

import javax.sql.ConnectionEvent;
import javax.sql.PooledConnection;

public class InfoStoreConnectionEvent extends ConnectionEvent {

    public InfoStoreConnectionEvent(PooledConnection con) {
        super(con);
    }
    
}
