package makson.search.solr.builder;

import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static makson.search.solr.builder.UrlBuilder.Protocol.*;

@NoArgsConstructor(staticName = "create")
public final class UrlBuilder {

    enum Protocol {

        WS("ws://"),
        HTTP("http://"),
        HTTPS("https://");

        private final String value;

        Protocol(String value) {
            this.value = value;
        }
    }

    private final List<String> params = new ArrayList<>();

    public UrlBuilder http() {
        params.add(HTTP.value);
        return this;
    }

    public UrlBuilder https() {
        params.add(HTTPS.value);
        return this;
    }

    public UrlBuilder ws() {
        params.add(WS.value);
        return this;
    }

    public UrlBuilder host(String host) {
        params.add(host);
        return this;
    }

    public UrlBuilder port(int port) {
        params.add(":");
        params.add(String.valueOf(port));
        return this;
    }

    public UrlBuilder baseUrl(String baseUrl) {
        for (Protocol protocol : Protocol.values()) {
            if (baseUrl.startsWith(protocol.value)) {
                baseUrl = baseUrl.substring(
                        protocol.value.length(),
                        baseUrl.length()
                );
            }
        }

        params.add(baseUrl);
        return this;
    }

    public UrlBuilder path(String path) {
        if (!path.startsWith("/")) {
            params.add("/");
        }
        params.add(path);
        return this;
    }

    @Nonnull
    public String getUrl() {
        return String.join("", params);
    }
}