import com.google.common.collect.ImmutableMap;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.junit.Test;
import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TimingOutJdbcConnections {

    /*
     Given a query will take longer than my timeout to complete
     When I attempt to run it
     Then the query is cancelled and the connection is either returned to the pool or closed entirely

     Given the health query will take longer than my timeout to complete
     When I attempt to run it
     Then the query is cancelled and the connection is closed entirely

     Given a connection is quietly having all its packets dropped
     When I attempt to use it
     Then the connection is closed entirely
     */
    static {
        TestLoggerFactory.getInstance().setPrintLevel(Level.INFO);
    }
    @Test
    public void connectionReleaseAfterTimeout() throws PropertyVetoException, InterruptedException, SQLException, IOException {
        Process exec = Runtime.getRuntime().exec("sudo iptables -D INPUT -p tcp --dport 3306 -m conntrack --ctstate ESTABLISHED -j DROP");
        exec.waitFor();
        final ComboPooledDataSource comboPooledDataSource = dataSource();
        while (comboPooledDataSource.getNumConnectionsDefaultUser() < 3) {
            System.out.println("Made " + comboPooledDataSource.getNumConnectionsDefaultUser() + " so far");
            Thread.sleep(5L);
        }
        System.out.println("About to run process");
        Process exec2 = Runtime.getRuntime().exec("sudo iptables -A INPUT -p tcp --dport 3306 -m conntrack --ctstate ESTABLISHED -j DROP");
        System.out.println("Running process");
        exec2.waitFor();
        System.out.println("Done");

        ExecutorService executorService = Executors.newCachedThreadPool();
        final List<PreparedStatement> statements = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            final int count = i;
            executorService.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    try (Connection conn = comboPooledDataSource.getConnection()) {
                        try (PreparedStatement preparedStatement = conn.prepareStatement("select 1")) {
                            statements.add(preparedStatement);
                            System.out.println("Executing " + count);
                            preparedStatement.execute();
                            System.out.println("Done " + count);
                        }
                    }
                    return "Done";
                }
            });
        }
        for (int i = 1; i <= 2; i++) {
            System.out.println(getStatistics(comboPooledDataSource));
            Thread.sleep(2000);
        }
        for (PreparedStatement statement : statements) {
            statement.cancel();
        }
        for (int i = 1; i <= 5; i++) {
            System.out.println(getStatistics(comboPooledDataSource));
            Thread.sleep(2000);
        }
    }

    public static final int IDLE_CONNECTION_TEST_PERIOD_SECONDS = 1;
    public static final int MAX_POOL_SIZE = 60;

    private ComboPooledDataSource dataSource() throws PropertyVetoException {
        final ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass("com.mysql.jdbc.Driver");
        dataSource.setJdbcUrl("jdbc:mysql://localhost:3306");
        dataSource.setUser("access");
        dataSource.setPassword("access");

        dataSource.setMaxPoolSize(5);
        dataSource.setMinPoolSize(3);

//        dataSource.setUnreturnedConnectionTimeout(1);

//        dataSource.setIdleConnectionTestPeriod(IDLE_CONNECTION_TEST_PERIOD_SECONDS);
//        dataSource.setPreferredTestQuery("SELECT 1");
//        dataSource.setTestConnectionOnCheckin(true);

        dataSource.setNumHelperThreads(MAX_POOL_SIZE / 5);
        return dataSource;
    }

    private static ImmutableMap<String, Long> getStatistics(ComboPooledDataSource dataSource) throws SQLException {
        return ImmutableMap.<String, Long>builder()
                .put( "total_connections",           (long) dataSource.getNumConnectionsDefaultUser())
                .put( "busy_connections",            (long) dataSource.getNumBusyConnectionsDefaultUser())
                .put( "idle_connections",            (long) dataSource.getNumIdleConnectionsDefaultUser())
                .put( "threads_awaiting_checkout",   (long) dataSource.getNumThreadsAwaitingCheckoutDefaultUser())
                .put( "unclosed_orphan_connections", (long) dataSource.getNumUnclosedOrphanedConnectionsDefaultUser())
                .put( "active_helper_threads",       (long) dataSource.getThreadPoolNumActiveThreads())
                .put( "idle_helper_threads",         (long) dataSource.getThreadPoolNumIdleThreads())
                .put( "tasks_pending",               (long) dataSource.getThreadPoolNumTasksPending())
                .put( "failed_checkins",                    dataSource.getNumFailedCheckinsDefaultUser())
                .put( "failed_checkouts",                   dataSource.getNumFailedCheckoutsDefaultUser())
                .put( "failed_idle_tests",                  dataSource.getNumFailedIdleTestsDefaultUser())
                .build();
    }
}
