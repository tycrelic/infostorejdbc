package com.github.tycrelic.infostorejdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InfoStoreDriver implements Driver {

  private static final int MAJOR_VERSION = 0;
  private static final int MINOR_VERSION = 1;
  private static final String VERSION_STRING = Integer.toString(MAJOR_VERSION) + "." + Integer.toString(MINOR_VERSION);
  private static final boolean JDBC_COMPLIANT = false;

  private static final String URL_PREFIX = "jdbc:infostore:";
  private static final int URL_PREFIX_LEN = URL_PREFIX.length();

  private static final DriverPropertyInfo[] PROPERTY_INFO;

  static {
    try {
      DriverManager.registerDriver(new InfoStoreDriver());
    } catch (SQLException ex) {
      Logger.getLogger(InfoStoreDriver.class.getName()).log(Level.SEVERE, null, ex);
    }

    final DriverPropertyInfo pi0 = new DriverPropertyInfo("user", null);
    pi0.required = true;
    final DriverPropertyInfo pi1 = new DriverPropertyInfo("password", null);
    pi1.required = true;
    final DriverPropertyInfo pi2 = new DriverPropertyInfo("host", null);
    pi2.required = true;
    final DriverPropertyInfo pi3 = new DriverPropertyInfo("authId", null);
    pi3.required = true;
    PROPERTY_INFO = new DriverPropertyInfo[]{
      pi0, pi1, pi2, pi3
    };
  }

  private InfoStoreDriver() {
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    return new InfoStoreConnection().connect(new InfoStoreDatabaseMetaData(url, info, VERSION_STRING, MAJOR_VERSION, MINOR_VERSION));
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url == null) {
      throw new SQLException("The url is null");
    }
    return url.startsWith(URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return PROPERTY_INFO.clone();//TODO
  }

  @Override
  public int getMajorVersion() {
    return MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return MINOR_VERSION;
  }

  @Override
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger(InfoStoreDriver.class.getName());//TODO
  }
}
