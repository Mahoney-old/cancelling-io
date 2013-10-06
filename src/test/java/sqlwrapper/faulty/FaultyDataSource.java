package sqlwrapper.faulty;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.threeten.bp.Duration;

import com.google.common.util.concurrent.Uninterruptibles;
import sqlwrapper.DataSourceWrapper;

public class FaultyDataSource extends DataSourceWrapper {

    private final Runnable workToDelayOpeningAConnection;

    public FaultyDataSource(DataSource decorated, Runnable workToDelayOpeningAConnection) {
        super(decorated);
        this.workToDelayOpeningAConnection = workToDelayOpeningAConnection;
    }

    @Override
    public Connection getConnection() throws SQLException {
        workToDelayOpeningAConnection.run();
        return super.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        workToDelayOpeningAConnection.run();
        return super.getConnection(username, password);
    }
}
