package com.github.tycrelic.infostorejdbc;

import com.crystaldecisions.sdk.exception.SDKException;
import com.crystaldecisions.sdk.framework.CrystalEnterprise;
import com.crystaldecisions.sdk.framework.IEnterpriseSession;
import com.crystaldecisions.sdk.framework.ISessionMgr;
import com.crystaldecisions.sdk.occa.infostore.IInfoStore;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InfoStoreConnection implements Connection {

  private boolean autoCommit;
  private boolean readOnly;
  private int transactionIsolation = TRANSACTION_SERIALIZABLE;
  private Map<String, Class<?>> typeMap;
  private int holdability;

  private IEnterpriseSession enterpriseSession;
  private IInfoStore infoStore;
  private InfoStoreDatabaseMetaData databaseMetaData;

  protected InfoStoreConnection() {
  }
  
  protected InfoStoreConnection connect(InfoStoreDatabaseMetaData databaseMetaData) throws SQLException {
    databaseMetaData.setConnection(this);
    
    Properties info=databaseMetaData.getInfo();
    String user = info.getProperty("user");
    String password = info.getProperty("password");
    String host = info.getProperty("host");
    String authId = info.getProperty("authId");

    IEnterpriseSession enterpriseSession = null;
    try {
      ISessionMgr sessionMgr = CrystalEnterprise.getSessionMgr();
      enterpriseSession = sessionMgr.logon(user, password, host, authId);
      setEnterpriseSession(enterpriseSession);
    } catch (SDKException ex) {
      close();
      throw new SQLException(ex);
    }

    this.databaseMetaData = databaseMetaData;
    
    return this;
  }
  
  @Override
  public Statement createStatement() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }

    //TODO
    return sql;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    this.autoCommit = autoCommit;
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    return autoCommit;
  }

  @Override
  public void commit() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    } else if (getAutoCommit()) {
      throw new SQLException("The Connection object is in auto-commit mode.");
    }
    try {
      infoStore.commit(null, autoCommit);
    } catch (SDKException ex) {
      throw new SQLException(ex);
    }
  }

  @Override
  public void rollback() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    } else if (getAutoCommit()) {
      throw new SQLException("The Connection object is in auto-commit mode.");
    }
    //do nothing
  }

  @Override
  public void close() throws SQLException {
    enterpriseSession.logoff();
    enterpriseSession = null;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return enterpriseSession == null;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }

    return databaseMetaData;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    //TODO
        /*
     else if(isInTransaction) {
     throw new SQLException("This method is called during a transaction.");
     }
     */
    this.readOnly = readOnly;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    return readOnly;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    //do nothing
  }

  @Override
  public String getCatalog() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    return null;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    } else {
      switch (level) {
        //case TRANSACTION_READ_UNCOMMITTED:
        case TRANSACTION_READ_COMMITTED:
        case TRANSACTION_REPEATABLE_READ:
        case TRANSACTION_SERIALIZABLE:
          transactionIsolation = level;
          break;
        default:
          throw new SQLException("The given parameter is not one of the Connection constants.");
      }
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    return transactionIsolation;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    //do nothing
  }

  private static void checkResultTypeConcurrency(int resultSetType, int resultSetConcurrency) throws SQLException {
    switch (resultSetType) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
      case ResultSet.TYPE_SCROLL_SENSITIVE:
        break;
      default:
        throw new SQLException("The given parameters are not ResultSet constants indicating type.");
    }
    switch (resultSetConcurrency) {
      case ResultSet.CONCUR_READ_ONLY:
      case ResultSet.CONCUR_UPDATABLE:
        break;
      default:
        throw new SQLException("The given parameters are not ResultSet constants indicating concurrency.");
    }
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    checkResultTypeConcurrency(resultSetType, resultSetConcurrency);
    return new InfoStoreStatement(this, resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    checkResultTypeConcurrency(resultSetType, resultSetConcurrency);
    return new InfoStorePreparedStatement(this, sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    checkResultTypeConcurrency(resultSetType, resultSetConcurrency);
    return new InfoStoreCallableStatement(this, sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }

    return typeMap == null ? (typeMap = new TreeMap()) : typeMap;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    typeMap = map;
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    this.holdability = holdability;
  }

  @Override
  public int getHoldability() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }

    return holdability;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    if (isClosed()) {
      throw new SQLException("The connection is closed.");
    }
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public String getSchema() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    if (isClosed()) {
      return;
    }

    if (executor == null) {
      throw new SQLException("Executor is null");
    }

    enterpriseSession.logoff();
    enterpriseSession = null;

    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          InfoStoreConnection.this.close();
        } catch (SQLException ex) {
          Logger.getLogger(InfoStoreConnection.class.getName()).log(Level.SEVERE, null, ex);
        }
      }

    });
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == InfoStoreConnection.class) {
      return iface.cast(this);
    } else if (iface == IEnterpriseSession.class) {
      return iface.cast(enterpriseSession);
    } else {
      throw new SQLException("Not a wrapper of " + iface.getName());
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface == InfoStoreConnection.class || iface == IEnterpriseSession.class;
  }

  /**
   * @return the enterpriseSession
   */
  public IEnterpriseSession getEnterpriseSession() {
    return enterpriseSession;
  }

  /**
   * @param enterpriseSession the enterpriseSession to set
   */
  public void setEnterpriseSession(IEnterpriseSession enterpriseSession) throws SDKException {
    this.enterpriseSession = enterpriseSession;
    this.infoStore = (IInfoStore) enterpriseSession.getService("InfoStore");
  }

  /**
   * @return the infoStore
   */
  public IInfoStore getInfoStore() {
    return infoStore;
  }

  /**
   * @param infoStore the infoStore to set
   */
  /*public void setInfoStore(IInfoStore infoStore) {
   this.infoStore = infoStore;
   }*/
}
