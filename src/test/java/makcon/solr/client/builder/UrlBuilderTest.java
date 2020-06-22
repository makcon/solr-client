package makcon.solr.client.builder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UrlBuilderTest {

    @Test
    public void buildCompleteUrl_onePath() {
        final String url = UrlBuilder.create()
                .http()
                .host("localhost")
                .port(8080)
                .path("path")
                .getUrl();

        assertEquals("http://localhost:8080/path", url);
    }

    @Test
    public void buildCompleteUrl_onePathWithSlash() {
        final String url = UrlBuilder.create()
                .http()
                .host("localhost")
                .port(8080)
                .path("/path")
                .getUrl();

        assertEquals("http://localhost:8080/path", url);
    }

    @Test
    public void buildCompleteUrl_twoPaths() {
        final String url = UrlBuilder.create()
                .http()
                .host("localhost")
                .port(8080)
                .path("path1")
                .path("/path2")
                .getUrl();

        assertEquals("http://localhost:8080/path1/path2", url);
    }

    @Test
    public void buildUrl_noPath() {
        final String url = UrlBuilder.create()
                .http()
                .host("localhost")
                .port(8080)
                .getUrl();

        assertEquals("http://localhost:8080", url);
    }

    @Test
    public void buildUrl_baseUrlWithoutProtocol() {
        final String url = UrlBuilder.create()
                .http()
                .baseUrl("localhost:8080")
                .getUrl();

        assertEquals("http://localhost:8080", url);
    }

    @Test
    public void buildUrl_baseUrlWithoutProtocolAndWithPath() {
        final String url = UrlBuilder.create()
                .http()
                .baseUrl("localhost:8080")
                .path("path")
                .getUrl();

        assertEquals("http://localhost:8080/path", url);
    }

    @Test
    public void buildUrl_baseUrlWithProtocolAndWithPath() {
        final String url = UrlBuilder.create()
                .http()
                .baseUrl("http://localhost:8080")
                .path("path")
                .getUrl();

        assertEquals("http://localhost:8080/path", url);
    }

    @Test
    public void buildUrl_baseUrlWithoutHttpsProtocol() {
        final String url = UrlBuilder.create()
                .https()
                .baseUrl("localhost:8080")
                .getUrl();

        assertEquals("https://localhost:8080", url);
    }

    @Test
    public void buildUrl_baseUrlWithoutWsProtocol() {
        final String url = UrlBuilder.create()
                .ws()
                .baseUrl("localhost:8080")
                .getUrl();

        assertEquals("ws://localhost:8080", url);
    }
}