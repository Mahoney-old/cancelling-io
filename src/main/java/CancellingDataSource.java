import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import com.github.tomakehurst.wiremock.common.Exceptions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import uk.org.lidalia.lang.Task;

public class CancellingDataSource {

    private final DataSource dataSource;
    private ExecutorService executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());

    public CancellingDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public TaskCancellingFuture<String> queryForString(final String sql) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                final Future<String> futureResponse = executorService.submit(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        try (final ResultSet resultSet = statement.executeQuery()) {
                            return "result";
                        }
                    }
                });
                return new TaskCancellingFuture<>(new Runnable() {
                    @Override
                    public void run() {
                        final Future<?> futureResponse = executorService.submit((Runnable) new Task() {
                            @Override
                            public void perform() throws Exception {
                                statement.cancel();
                            }
                        });
                        try {
                            futureResponse.get(100, TimeUnit.MILLISECONDS);
                        } catch (Exception e) {
                            try {
                                conn.abort(executorService);
                            } catch (SQLException e1) {
                                Exceptions.throwUnchecked(e);
                            }
                        }
                    }
                }, futureResponse);
            }
        }
    }
}
