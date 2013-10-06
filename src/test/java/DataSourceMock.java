import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.threeten.bp.Duration;
import org.threeten.bp.Instant;

import com.google.common.util.concurrent.Uninterruptibles;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static org.threeten.bp.Instant.now;

public class DataSourceMock implements DataSource {

    private final Deque<Connection> connections;
    private final Duration timeToRetrieveConnection;

    public DataSourceMock(Duration timeToRetrieveConnection, Connection... connections) {
        this.timeToRetrieveConnection = timeToRetrieveConnection;
        this.connections = new ArrayDeque<>(asList(connections));
    }

    @Override
    public Connection getConnection() throws SQLException {
        Instant start = now();
        Instant queryCompleted = start.plus(timeToRetrieveConnection);
        while (now().isBefore(queryCompleted)) {
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
        }
        return checkNotNull(connections.pop());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
