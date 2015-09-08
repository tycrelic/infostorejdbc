package com.github.tycrelic.infostorejdbc;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;

public class InfoStoreConnectionEventListener implements ConnectionEventListener {

    @Override
    public void connectionClosed(ConnectionEvent event) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void connectionErrorOccurred(ConnectionEvent event) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
