package com.github.tycrelic.infostorejdbc;

import com.crystaldecisions.sdk.exception.SDKException;
import com.crystaldecisions.sdk.framework.IEnterpriseSession;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Properties;

public class InfoStoreDatabaseMetaData implements DatabaseMetaData {

  public static final String DATABASE_PRODUCT_NAME = "Infostore";
  public static final String DRIVER_NAME = "InfoStoreJDBC";

  private String url;
  private Properties info;

  private InfoStoreConnection conn;
  private IEnterpriseSession enterpriseSession;

  private String driverVersion;
  private int driverMajorVersion;
  private int driverMinorVersion;

  public InfoStoreDatabaseMetaData(String url, Properties info, String driverVersion, int driverMajorVersion, int driverMinorVersion) {
    this.url = url;
    this.info = info;
    this.driverVersion = driverVersion;
    this.driverMajorVersion = driverMajorVersion;
    this.driverMinorVersion = driverMinorVersion;

  }

  protected void setConnection(InfoStoreConnection conn) {
    this.conn = conn;
    this.enterpriseSession = conn.getEnterpriseSession();
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return false;//TODO
  }

  @Override
  public String getURL() throws SQLException {
    return url;
  }

  @Override
  public String getUserName() throws SQLException {
    try {
      return enterpriseSession.getUserInfo().getUserName();
    } catch (SDKException ex) {
      throw new SQLException(ex);
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return true;//TODO
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return true;//TODO
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return DATABASE_PRODUCT_NAME;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    try {
      int enterpriseVersion = enterpriseSession.getEnterpriseVersion();
      switch (enterpriseVersion) {
        case IEnterpriseSession.CeEnterpriseVersion.VERSION8_0:
          return "8.0";
        case IEnterpriseSession.CeEnterpriseVersion.VERSION8_5:
          return "8.5";
        case IEnterpriseSession.CeEnterpriseVersion.VERSION9_0:
          return "9.0";
        case IEnterpriseSession.CeEnterpriseVersion.VERSION10_0:
          return "10.0";
        case IEnterpriseSession.CeEnterpriseVersion.VERSION11_0:
          return "11.0";
        default:
          return Integer.toString(enterpriseVersion / 100) + "." + Integer.toString(enterpriseVersion % 100);
      }
    } catch (SDKException ex) {
      throw new SQLException(ex);
    }
  }

  @Override
  public String getDriverName() throws SQLException {
    return DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    if (driverVersion == null) {
      driverVersion = getDriverMajorVersion() + "." + getDriverMinorVersion();
    }
    return driverVersion;
  }

  @Override
  public int getDriverMajorVersion() {
    return driverMajorVersion;
  }

  @Override
  public int getDriverMinorVersion() {
    return driverMinorVersion;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return " ";//TODO
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return "";//TODO
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return "";//TODO
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return "";//TODO
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return "";//TODO
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "";//TODO
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return "";//TODO
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";//TODO
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return "procedure";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "catalog";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return false;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return true;//TODO
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return true;//TODO
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return 0;//TODO
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return 0;//TODO
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_READ_COMMITTED;//TODO
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return true;//TODO
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    switch (level) { //TODO
      case Connection.TRANSACTION_READ_COMMITTED:
        return true;
      case Connection.TRANSACTION_NONE:
      case Connection.TRANSACTION_READ_UNCOMMITTED:
      case Connection.TRANSACTION_REPEATABLE_READ:
      case Connection.TRANSACTION_SERIALIZABLE:
      default:
        return false;
    }
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return true;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return true;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    switch (type) {//TODO
      case ResultSet.TYPE_FORWARD_ONLY:
        return true;
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
      case ResultSet.TYPE_SCROLL_SENSITIVE:
      default:
        return false;
    }
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return supportsResultSetType(type) && (concurrency==ResultSet.CONCUR_READ_ONLY || concurrency==ResultSet.CONCUR_UPDATABLE);//TODO
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return true;//TODO
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public Connection getConnection() throws SQLException {
    return conn;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;//TODO
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;//TODO
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    try {
      return enterpriseSession.getEnterpriseVersion()/100;
    } catch (SDKException ex) {
      throw new SQLException(ex);
    }
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    try {
      return enterpriseSession.getEnterpriseVersion()%100;
    } catch (SDKException ex) {
      throw new SQLException(ex);
    }
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 4;//TODO
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 2;//TODO
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL;//TODO
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return true;//TODO
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;//TODO
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return RowIdLifetime.ROWID_UNSUPPORTED;//TODO
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    return new InfoStoreResultSet(null);//TODO
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;//TODO
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == InfoStoreDatabaseMetaData.class) {
      return iface.cast(this);
    } else {
      throw new SQLException("Not a wrapper of " + iface.getName());
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface == InfoStoreDatabaseMetaData.class;
  }

  /**
   * @return the info
   */
  public Properties getInfo() {
    return info;
  }

  /**
   * @param info the info to set
   */
  public void setInfo(Properties info) {
    this.info = info;
  }

}
