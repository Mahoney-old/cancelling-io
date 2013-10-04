import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.junit.Rule;
import org.junit.Test;
import org.threeten.bp.Duration;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.exception.HystrixRuntimeException;

import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

public class ClientUser {

    static {
        TestLoggerFactory.getInstance().setPrintLevel(Level.TRACE);
    }
    @Rule
    public WireMockRule wireMockRule = new WireMockRule();


    @Test
    public void cancellableHttpClient() throws IOException, InterruptedException, ExecutionException {

        stubFor(get(urlEqualTo("/blah")).willReturn(aResponse().withFixedDelay((int) Duration.ofMinutes(10).toMillis())));

        final CancellableHttpClient httpClient = new CancellableHttpClient();

        try {
            final StatusLine statusLine = new GetBlahCommand(httpClient).execute();
            fail("Should have timed out!");
        } catch (HystrixRuntimeException hre) {
            Thread.sleep(100L);
            assertThat(httpClient.getStats().getLeased(), is(0));
            assertThat(httpClient.getStats().getPending(), is(0));
        }

        try {
            final StatusLine statusLine = new GetBlahCommand(httpClient).execute();
            fail("Should have timed out!");
        } catch (HystrixRuntimeException hre) {
            Thread.sleep(100L);
            assertThat(httpClient.getStats().getLeased(), is(0));
            assertThat(httpClient.getStats().getPending(), is(0));
        }
    }

    private class GetBlahCommand extends HystrixCommand<StatusLine> {

        private final CancellableHttpClient httpClient;

        public GetBlahCommand(CancellableHttpClient httpClient) {
            super(HystrixCommandGroupKey.Factory.asKey("blah"));
            this.httpClient = httpClient;
        }

        @Override
        protected StatusLine run() throws Exception {
            final HttpGet request = new HttpGet("http://localhost:8080/blah");
            final TaskCancellingFuture<HttpResponse> response = httpClient.execute(request);
            try {
                return response.get().getStatusLine();
            } finally {
                response.cancel();
            }
        }
    }
}
