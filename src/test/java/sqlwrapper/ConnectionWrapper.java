package sqlwrapper;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConnectionWrapper implements Connection {
    private final Connection decorated;
    private final DecoratorFactory<PreparedStatement, ConnectionWrapper> preparedStatementFactory;

    public ConnectionWrapper(Connection decorated) {
        this(decorated, new NoOpDecoratorFactory<PreparedStatement, ConnectionWrapper>());
    }

    public ConnectionWrapper(Connection decorated, DecoratorFactory<PreparedStatement, ConnectionWrapper> preparedStatementFactory) {
        this.decorated = checkNotNull(decorated);
        this.preparedStatementFactory = checkNotNull(preparedStatementFactory);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return decorated.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return preparedStatementFactory.decorate(decorated.prepareStatement(sql), this);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return decorated.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return decorated.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        decorated.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return decorated.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        decorated.commit();
    }

    @Override
    public void rollback() throws SQLException {
        decorated.rollback();
    }

    @Override
    public void close() throws SQLException {
        decorated.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return decorated.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return decorated.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        decorated.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return decorated.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        decorated.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return decorated.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        decorated.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return decorated.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return decorated.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        decorated.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return decorated.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return decorated.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return decorated.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return decorated.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        decorated.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        decorated.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return decorated.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return decorated.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return decorated.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        decorated.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        decorated.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return decorated.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return preparedStatementFactory.decorate(decorated.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), this);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return decorated.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return preparedStatementFactory.decorate(decorated.prepareStatement(sql, autoGeneratedKeys), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return preparedStatementFactory.decorate(decorated.prepareStatement(sql, columnIndexes), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return preparedStatementFactory.decorate(decorated.prepareStatement(sql, columnNames), this);
    }

    @Override
    public Clob createClob() throws SQLException {
        return decorated.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return decorated.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return decorated.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return decorated.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return decorated.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        decorated.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        decorated.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return decorated.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return decorated.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return decorated.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return decorated.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        decorated.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return decorated.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        decorated.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        decorated.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return decorated.getNetworkTimeout();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        return decorated.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return (iface.isInstance(this) || decorated.isWrapperFor(iface));
    }
}
