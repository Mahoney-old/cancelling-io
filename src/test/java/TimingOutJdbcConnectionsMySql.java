import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import sqlwrapper.ConnectionWrapper;
import sqlwrapper.DataSourceWrapper;
import sqlwrapper.DecoratorFactory;
import sqlwrapper.PreparedStatementWrapper;
import sqlwrapper.faulty.FaultyDataSource;
import sqlwrapper.noop.NoOpConnection;
import sqlwrapper.noop.NoOpDataSource;
import sqlwrapper.noop.NoOpPreparedStatement;
import sqlwrapper.noop.NoOpResultSet;

import uk.org.lidalia.lang.Exceptions;
import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.org.lidalia.test.ShouldThrow.shouldThrow;

@RunWith(Parameterized.class)
public class TimingOutJdbcConnectionsMySql {

    private static final String LINUX_START_MY_SQL = "service mysql start";
    private static final String LINUX_REMOVE_PACKET_DROPPING = "sudo iptables -D INPUT -p tcp --dport 3306 -m conntrack --ctstate ESTABLISHED -j DROP";
    private static final String LINUX_ENABLE_PACKET_DROPPING = "sudo iptables -A INPUT -p tcp --dport 3306 -m conntrack --ctstate ESTABLISHED -j DROP";

    private static final String MAC_START_MY_SQL = "/usr/local/bin/mysql.server start";
    private static final String MAC_REMOVE_PACKET_DROPPING = "sudo iptables -D INPUT -p tcp --dport 3306 -m conntrack --ctstate ESTABLISHED -j DROP";
    private static final String MAC_ENABLE_PACKET_DROPPING = "sudo iptables -A INPUT -p tcp --dport 3306 -m conntrack --ctstate ESTABLISHED -j DROP";
    private static WithConnectionAndStatement sleepWhileNotAbortedOrCancelled;

    private final DataSource dataSource;
    private final Runnable workToDelayOpeningAConnection;
    private final WithConnectionAndStatement workToDelayExecutingAQuery;
    private final WithConnectionAndStatement workToDelayCancellingAQuery;
    private final WithConnectionAndStatement workToDelayPreparingAStatement;

    public TimingOutJdbcConnectionsMySql(DataSource dataSource,
                                         Runnable workToDelayOpeningAConnection,
                                         WithConnectionAndStatement workToDelayExecutingAQuery) {
        this.dataSource = dataSource;
        this.workToDelayOpeningAConnection = workToDelayOpeningAConnection;
        this.workToDelayExecutingAQuery = workToDelayExecutingAQuery;
        this.workToDelayCancellingAQuery = workToDelayExecutingAQuery;
        this.workToDelayPreparingAStatement = workToDelayExecutingAQuery;
    }

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


    @Test
    public void openingAConnectionTakesTooLong() throws SQLException {
        final DataSource dataSource = new FaultyDataSource(this.dataSource, workToDelayOpeningAConnection);
        final HystrixRuntimeException hystrixRuntimeException = shouldThrow(HystrixRuntimeException.class, new Runnable() {
            @Override
            public void run() {
                new SimpleQueryCommand(dataSource).execute();
            }
        });
        assertThat(hystrixRuntimeException.getCause(), is(instanceOf(TimeoutException.class)));
    }

    @Test
    public void preparingAStatementTakesTooLong() throws SQLException, InterruptedException {
        // given a query which will block forever
        // and cancelling it will block forever
        final AtomicReference<AbortedAwareConnection> connection = new AtomicReference<>();
        final DataSource dataSource = new DataSourceWrapper(this.dataSource, new DecoratorFactory<Connection, DataSourceWrapper>() {
            @Override
            public Connection decorate(Connection toDecorate, DataSourceWrapper dataSourceWrapper) {
                connection.set(new AbortedAwareConnection(toDecorate) {
                    @Override
                    public PreparedStatement prepareStatement(String sql) throws SQLException {
                        workToDelayPreparingAStatement.with(this, null);
                        return super.prepareStatement(sql);
                    }
                });
                return connection.get();
            }
        });
        // when the timeout occurs
        final HystrixRuntimeException hystrixRuntimeException = shouldThrow(HystrixRuntimeException.class, new Runnable() {
            @Override
            public void run() {
                new SimpleQueryCommand(dataSource).execute();
            }
        });
        assertThat(hystrixRuntimeException.getCause(), is(instanceOf(TimeoutException.class)));

        Thread.sleep(200L);
        // then the connection is aborted
        assertTrue(connection.get().isAborted());
    }

    @Test
    public void runningAQueryTakesTooLongButCancelWorks() throws SQLException, InterruptedException {
        // given a query will run for longer than a Hystrix timeout
        final AtomicReference<AbortedAwareConnection> connection = new AtomicReference<>();
        final AtomicReference<CancellationAwarePreparedStatement> preparedStatement = new AtomicReference<>();
        final DataSource dataSource = new DataSourceWrapper(this.dataSource, new DecoratorFactory<Connection, DataSourceWrapper>() {
            @Override
            public Connection decorate(Connection toDecorate, DataSourceWrapper dataSourceWrapper) {
                final DecoratorFactory<PreparedStatement, ConnectionWrapper> preparedStatementFactory = new DecoratorFactory<PreparedStatement, ConnectionWrapper>() {
                    @Override
                    public PreparedStatement decorate(PreparedStatement toDecorate, final ConnectionWrapper connectionWrapper) {
                        preparedStatement.set(new CancellationAwarePreparedStatement(toDecorate) {
                            @Override
                            public ResultSet executeQuery() throws SQLException {
                                workToDelayExecutingAQuery.with((AbortedAwareConnection) connectionWrapper, this);
                                return super.executeQuery();
                            }
                        });
                        return preparedStatement.get();
                    }
                };
                final AbortedAwareConnection abortedAwareConnection = new AbortedAwareConnection(toDecorate, preparedStatementFactory);
                connection.set(abortedAwareConnection);
                return connection.get();
            }
        });

        // when the timeout occurs
        final HystrixRuntimeException hystrixRuntimeException = shouldThrow(HystrixRuntimeException.class, new Runnable() {
            @Override
            public void run() {
                new SimpleQueryCommand(dataSource).execute();
            }
        });
        assertThat(hystrixRuntimeException.getCause(), is(instanceOf(TimeoutException.class)));

        Thread.sleep(200L);
        // then the query is cancelled
        assertTrue(preparedStatement.get().isCancelled());
        assertTrue(connection.get().isClosed());
        assertTrue(preparedStatement.get().isClosed());
        assertFalse(connection.get().isAborted());
    }

    @Test
    public void runningAQueryTakesTooLongAndSoDoesCancellingIt() throws SQLException, InterruptedException {
        // given a query which will block forever
        // and cancelling it will block forever
        final AtomicReference<AbortedAwareConnection> connection = new AtomicReference<>();
        final DataSource dataSource = new DataSourceWrapper(this.dataSource, new DecoratorFactory<Connection, DataSourceWrapper>() {
            @Override
            public Connection decorate(Connection toDecorate, DataSourceWrapper dataSourceWrapper) {
                connection.set(new AbortedAwareConnection(toDecorate, new DecoratorFactory<PreparedStatement, ConnectionWrapper>() {
                    @Override
                    public PreparedStatement decorate(PreparedStatement toDecorate, final ConnectionWrapper connectionWrapper) {
                        return new CancellationAwarePreparedStatement(toDecorate) {
                            @Override
                            public ResultSet executeQuery() throws SQLException {
                                workToDelayExecutingAQuery.with((AbortedAwareConnection) connectionWrapper, this);
                                return super.executeQuery();
                            }
                            @Override
                            public void cancel() throws SQLException {
                                workToDelayCancellingAQuery.with((AbortedAwareConnection) connectionWrapper, this);
                                super.cancel();
                            }
                        };
                    }
                }));
                return connection.get();
            }
        });
        // when the timeout occurs
        final HystrixRuntimeException hystrixRuntimeException = shouldThrow(HystrixRuntimeException.class, new Runnable() {
            @Override
            public void run() {
                new SimpleQueryCommand(dataSource).execute();
            }
        });
        assertThat(hystrixRuntimeException.getCause(), is(instanceOf(TimeoutException.class)));

        // then the connection is aborted
        Thread.sleep(200L);
        assertTrue(connection.get().isAborted());
    }

    @Test
    public void runningAQueryTakesTooLongAndCancellingItThrowsException() throws InterruptedException {
        // given a query which will block forever
        // and cancelling it throws an exception
        final AtomicReference<AbortedAwareConnection> connection = new AtomicReference<>();
        final DataSource dataSource = new DataSourceWrapper(this.dataSource, new DecoratorFactory<Connection, DataSourceWrapper>() {
            @Override
            public Connection decorate(Connection toDecorate, DataSourceWrapper dataSourceWrapper) {
                connection.set(new AbortedAwareConnection(toDecorate, new DecoratorFactory<PreparedStatement, ConnectionWrapper>() {
                    @Override
                    public PreparedStatement decorate(PreparedStatement toDecorate, final ConnectionWrapper connectionWrapper) {
                        return new CancellationAwarePreparedStatement(toDecorate) {
                            @Override
                            public ResultSet executeQuery() throws SQLException {
                                workToDelayExecutingAQuery.with((AbortedAwareConnection) connectionWrapper, this);
                                return super.executeQuery();
                            }
                            @Override
                            public void cancel() throws SQLException {
                                throw new SQLFeatureNotSupportedException("Cannot be cancelled");
                            }
                        };
                    }
                }));
                return connection.get();
            }
        });
        // when the timeout occurs
        final HystrixRuntimeException hystrixRuntimeException = shouldThrow(HystrixRuntimeException.class, new Runnable() {
            @Override
            public void run() {
                new SimpleQueryCommand(dataSource).execute();
            }
        });
        // then the connection is aborted
        Thread.sleep(200L);
        assertTrue(connection.get().isAborted());
    }

    private static class SimpleQueryCommand extends HystrixCommand<Integer> {
        private final DataSource dataSource;
        SimpleQueryCommand(DataSource dataSource) {
            super(Setter.withGroupKey(
                    HystrixCommandGroupKey.Factory.asKey("keyname"))
                    .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
//                            .withExecutionIsolationThreadTimeoutInMilliseconds(2000)
                    )
            );
            this.dataSource = dataSource;
        }

        @Override
        protected Integer run() throws Exception {
            final ExecutorService executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
            try (Connection conn = dataSource.getConnection()) {
                try (PreparedStatement statement = getPreparedStatement(conn, executorService)) {
                    final Future<Integer> result = executorService.submit(new Callable<Integer>() {
                        @Override
                        public Integer call() throws Exception {
                            final ResultSet resultSet = statement.executeQuery();
                            resultSet.next();
                            return resultSet.getInt(1);
                        }
                    });
                    try {
                        return result.get();
                    } catch (InterruptedException ie) {
                        result.cancel(true);
                        final Future<Void> cancelled = executorService.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                statement.cancel();
                                return null;
                            }
                        });
                        try {
                            cancelled.get(100, TimeUnit.MILLISECONDS);
                        } catch (Exception e) {
                            conn.abort(executorService);
                        }
                        throw new SQLException("Communication timed out!");
                    }
                }
            }
        }

        private PreparedStatement getPreparedStatement(final Connection conn, ExecutorService executorService) throws SQLException, ExecutionException {
            final Future<PreparedStatement> submit = executorService.submit(new Callable<PreparedStatement>() {
                @Override
                public PreparedStatement call() throws Exception {
                    return conn.prepareStatement("select 42");
                }
            });
            try {
                return submit.get();
            } catch (InterruptedException ie) {
                submit.cancel(true);
                conn.abort(executorService);
                throw new SQLException("Timed out preparing statement");
            }
        }
    }

    static {
        TestLoggerFactory.getInstance().setPrintLevel(Level.INFO);
    }

    @Before @After
    public void resetConnections() throws IOException, InterruptedException {
        runCommand(LINUX_REMOVE_PACKET_DROPPING);
    }

    @Before
    public void startMySql() throws IOException, InterruptedException {
        runCommand(MAC_START_MY_SQL);
    }

    private static void runCommand(String command) throws InterruptedException, IOException {
        final int exitCode = Runtime.getRuntime().exec(command).waitFor();
//        assertThat(exitCode, is(0));
    }

    private static MysqlDataSource mysqlDataSource() {
        final MysqlDataSource decorated = new MysqlDataSource();
        decorated.setServerName("localhost");
        decorated.setUser("root");
        return decorated;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        sleepWhileNotAbortedOrCancelled = new WithConnectionAndStatement() {
            @Override
            public void with(AbortedAwareConnection connection, CancellationAwarePreparedStatement preparedStatement) throws SQLException {
                while (!connection.isAborted() && (preparedStatement == null || !preparedStatement.isCancelled())) {
                    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
                }
                if (connection.isAborted()) {
                    throw new SQLException("Connection aborted");
                }
                if (preparedStatement != null && preparedStatement.isCancelled()) {
                    throw new SQLException("Prepared Statement cancelled");
                }
            }
        };
        return asList(new Object[][] {
//                { mysqlDataSource(), silentlyDropCommunicationWithMySql },
                {
                        mockDataSource(),
                        sleepForever,
                        sleepWhileNotAbortedOrCancelled
                }
        });
    }

    private static DataSource mockDataSource() {
        return new NoOpDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                return new NoOpConnection() {
                    @Override
                    public PreparedStatement prepareStatement(String sql) throws SQLException {
                        return new NoOpPreparedStatement(this) {
                            @Override
                            public ResultSet executeQuery() {
                                return new NoOpResultSet() {
                                    @Override
                                    public int getInt(int column) {
                                        return 42;
                                    }
                                };
                            }
                        };
                    }
                };
            }
        };
    }

    private static Runnable sleepForever = new Runnable() {
        @Override
        public void run() {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.DAYS);
        }
    };

    private static Runnable silentlyDropCommunicationWithMySql = new Runnable() {
        @Override
        public void run() {
            try {
                runCommand(LINUX_ENABLE_PACKET_DROPPING);
            } catch (Exception e) {
                Exceptions.throwUnchecked(e);
            }
        }
    };

    private static class CancellationAwarePreparedStatement extends PreparedStatementWrapper {
        public CancellationAwarePreparedStatement(PreparedStatement decorated) {
            super(decorated);
        }

        private volatile boolean cancelled = false;

        @Override
        public void cancel() throws SQLException {
            super.cancel();
            cancelled = true;
        }

        private boolean isCancelled() {
            return cancelled;
        }
    }

    private class AbortedAwareConnection extends ConnectionWrapper {

        public AbortedAwareConnection(Connection decorated) {
            super(decorated);
        }

        public AbortedAwareConnection(Connection decorated, DecoratorFactory<PreparedStatement, ConnectionWrapper> preparedStatementFactory) {
            super(decorated, preparedStatementFactory);
        }

        private volatile boolean aborted = false;

        @Override
        public void abort(Executor executor) throws SQLException {
            super.abort(executor);
            aborted = true;
        }

        private boolean isAborted() {
            return aborted;
        }
    }

    private interface WithConnectionAndStatement {
        void with(AbortedAwareConnection connection, CancellationAwarePreparedStatement preparedStatement) throws SQLException;
    }
}
