import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.http.client.methods.HttpUriRequest;

public class CancellableFutureResponse<V> implements Future<V> {

    private final HttpUriRequest request;
    private final Future<V> futureResponse;

    public CancellableFutureResponse(HttpUriRequest request, Future<V> futureResponse) {
        this.request = request;
        this.futureResponse = futureResponse;
    }

    public boolean cancel() {
        return cancel(false);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        request.abort();
        return futureResponse.cancel(mayInterruptIfRunning);
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return futureResponse.get(timeout, unit);
        } finally {
            request.abort();
        }
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        try {
            return futureResponse.get();
        } finally {
            request.abort();
        }
    }

    public V getUninterruptibly() throws ExecutionException {
        try {
            return Uninterruptibles.getUninterruptibly(futureResponse);
        } finally {
            request.abort();
        }
    }

    public V getUninterruptibly(long timeout, TimeUnit unit) throws ExecutionException, TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(futureResponse, timeout, unit);
        } finally {
            request.abort();
        }
    }

    @Override
    public boolean isCancelled() {
        return futureResponse.isCancelled();
    }

    @Override
    public boolean isDone() {
        return futureResponse.isDone();
    }
}
