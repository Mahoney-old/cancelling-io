import com.google.common.util.concurrent.Uninterruptibles;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TaskCancellingFuture<V> implements Future<V> {

    private final Runnable mechanismToCancelTask;
    private final Future<V> futureResponse;

    public TaskCancellingFuture(Runnable mechanismToCancelTask, Future<V> futureResponse) {
        this.mechanismToCancelTask = mechanismToCancelTask;
        this.futureResponse = futureResponse;
    }

    public boolean cancel() {
        return cancel(false);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        mechanismToCancelTask.run();
        return futureResponse.cancel(mayInterruptIfRunning);
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return futureResponse.get(timeout, unit);
        } finally {
            mechanismToCancelTask.run();
        }
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        try {
            return futureResponse.get();
        } finally {
            mechanismToCancelTask.run();
        }
    }

    public V getUninterruptibly() throws ExecutionException {
        try {
            return Uninterruptibles.getUninterruptibly(futureResponse);
        } finally {
            mechanismToCancelTask.run();
        }
    }

    public V getUninterruptibly(long timeout, TimeUnit unit) throws ExecutionException, TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(futureResponse, timeout, unit);
        } finally {
            mechanismToCancelTask.run();
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
