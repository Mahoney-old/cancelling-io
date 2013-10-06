package sqlwrapper;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import static com.google.common.base.Preconditions.checkNotNull;

public class DataSourceWrapper implements DataSource {

    private final DataSource decorated;
    private final DecoratorFactory<Connection, DataSourceWrapper> connectionFactory;

    public DataSourceWrapper(DataSource decorated) {
        this(decorated, new NoOpDecoratorFactory<Connection, DataSourceWrapper>());
    }

    public DataSourceWrapper(DataSource decorated, DecoratorFactory<Connection, DataSourceWrapper> connectionFactory) {
        this.decorated = checkNotNull(decorated);
        this.connectionFactory = checkNotNull(connectionFactory);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connectionFactory.decorate(decorated.getConnection(), this);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return connectionFactory.decorate(decorated.getConnection(username, password), this);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return decorated.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        decorated.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        decorated.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return decorated.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return decorated.getParentLogger();
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
