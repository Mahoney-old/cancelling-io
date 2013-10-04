import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class CancellableHttpClient {

    private PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
    private CloseableHttpClient httpClient = HttpClients.custom().setConnectionManager(manager).build();
    private ExecutorService executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());

    public TaskCancellingFuture<HttpResponse> execute(final HttpUriRequest request) {
        final Future<HttpResponse> futureResponse = executorService.submit(new Callable<HttpResponse>() {
            @Override
            public HttpResponse call() throws Exception {
                return httpClient.execute(request);
            }
        });
        return new TaskCancellingFuture<>(new Runnable() {
            @Override
            public void run() {
                request.abort();
            }
        }, futureResponse);
    }

    public PoolStats getStats() {
        return manager.getTotalStats();
    }
}
