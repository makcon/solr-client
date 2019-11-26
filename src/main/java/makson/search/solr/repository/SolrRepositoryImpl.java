package makson.search.solr.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import makson.search.solr.builder.FQ;
import makson.search.solr.builder.UrlBuilder;
import makson.search.solr.constant.QueryParams;
import makson.search.solr.dataobject.SolrDoc;
import makson.search.solr.dataobject.SolrInputDoc;
import makson.search.solr.exception.RepositoryAccessException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION.COMMIT;

@RequiredArgsConstructor
@Slf4j
public class SolrRepositoryImpl implements SolrRepository {

    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 1000;
    private static final int DEFAULT_SOCKET_TIMEOUT_MS = 30000;

    private final SolrClient solrClient;
    private final String url;
    private final boolean softCommit;

    protected SolrRepositoryImpl(SolrClient solrClient, boolean softCommit) {
        this(solrClient, null, softCommit);
    }

    public SolrRepositoryImpl(String solrHost,
                              Integer solrPort,
                              String corePath) {
        this(UrlBuilder.create()
                .http()
                .host(solrHost)
                .port(solrPort)
                .path(corePath)
                .getUrl()
        );
    }

    public SolrRepositoryImpl(String baseUrl,
                              String corePath) {
        this(UrlBuilder.create()
                .http()
                .baseUrl(baseUrl)
                .path(corePath)
                .getUrl()
        );
    }

    public SolrRepositoryImpl(String url) {
        this(
                url,
                DEFAULT_CONNECTION_TIMEOUT_MS,
                DEFAULT_SOCKET_TIMEOUT_MS
        );
    }

    public SolrRepositoryImpl(String url,
                              int socketTimeout) {
        this(
                url,
                DEFAULT_CONNECTION_TIMEOUT_MS,
                socketTimeout
        );
    }

    public SolrRepositoryImpl(String url,
                              int connectionTimeout,
                              int socketTimeout) {
        this.solrClient = createHttpsolrClient(
                url,
                connectionTimeout,
                socketTimeout
        );
        this.softCommit = true;
        this.url = url;
    }

    private SolrClient createHttpsolrClient(String url,
                                            int connectionTimeout,
                                            int socketTimeout) {
        SolrClient httpsolrClient = new HttpSolrClient.Builder(url)
                .withConnectionTimeout(connectionTimeout)
                .withSocketTimeout(socketTimeout)
                .build();

        logger.info(
                "Created solrClient with timeouts: connection: {} and socket: {}",
                connectionTimeout,
                socketTimeout
        );

        return httpsolrClient;
    }

    @Override
    public void shutdown() {
        try {
            solrClient.close();
        } catch (IOException e) {
            logger.error("Failed to shutdown solr client", e);
        }
    }

    @Override
    public void save(SolrInputDoc doc) {
        save(doc, false, null, null);
    }

    @Override
    public void save(SolrInputDoc doc,
                     Integer commitWithin) {
        save(doc, false, commitWithin, null);
    }

    @Override
    public void save(SolrInputDoc doc,
                     Integer commitWithin,
                     String route) throws RepositoryAccessException {
        save(doc, false, commitWithin, route);
    }

    @Override
    public void save(SolrInputDoc doc,
                     boolean softCommit) {
        save(doc, softCommit,null,null);
    }

    @Override
    public void save(SolrInputDoc doc,
                     boolean softCommit,
                     String route) throws RepositoryAccessException {
        save(doc, softCommit, null, route);
    }

    @Override
    public void save(List<SolrInputDoc> documents) throws RepositoryAccessException {
        logger.info("Starting to add documents in Solr: {}, documents: {}", url, documents);
        try {
            List<SolrInputDocument> solrDocuments = documents.stream()
                    .map(SolrInputDoc::getSolrInputDocument)
                    .collect(toList());

            process(solrDocuments);

            softCommit();
            logger.info("Documents have been successfully added in Solr: {}", url);
        } catch (Exception e) {
            logger.error("Can't save documents in Solr: {}", url);
            throw new RepositoryAccessException("Can't save documents in Solr: " + url, e);
        }
    }

    @Override
    public void deleteById(String id) {
        deleteByIds(Collections.singletonList(id));
    }

    @Override
    public void deleteByIds(List<String> ids) {
        logger.info("Deleting documents from the Solr: {}, document IDs: {}", url, ids);
        try {
            UpdateRequest updateRequest = createRequest();

            updateRequest.deleteById(ids).process(solrClient);
            softCommit();
            logger.info("Documents {} have been successfully deleted from Solr: {}", ids, url);
        } catch (Exception e) {
            logger.error("Can't delete documents for IDs {} from Solr: {}", ids, url);
            throw new RepositoryAccessException("Can't delete documents for ids " + ids + " from the Solr: " + url, e);
        }
    }

    @Override
    public void deleteByQuery(String query) throws RepositoryAccessException {
        logger.info("Deleting documents from Solr: {}, deletion query: {}", url, query);
        try {
            UpdateRequest updateRequest = createRequest();

            updateRequest.deleteByQuery(query).process(solrClient);
            softCommit();
            logger.info("Documents have been successfully deleted from Solr: {}, by query: {}", url, query);
        } catch (Exception e) {
            logger.error("Can't delete documents by query {} from Solr: {}", query, url);
            throw new RepositoryAccessException("Can't delete documents by query " + query + " from Solr: " + url, e);
        }
    }

    @Override
    public Optional<SolrDoc> find(SolrQuery query) {
        List<SolrDoc> solrDocs = findList(query);

        return solrDocs.isEmpty() ?
                Optional.empty() : Optional.of(solrDocs.get(0));
    }

    @Override
    public List<SolrDoc> findList(SolrQuery query) {
        return findByQuery(query)
                .getResults()
                .stream()
                .map(SolrDoc::new)
                .collect(toList());
    }

    @Override
    public QueryResponse findByQuery(SolrQuery query) {
        logger.info("Sending Solr query to server: {}/select?{}", url, query);
        try {
            QueryResponse queryResponse = solrClient.query(query, POST);
            logger.debug("Finished to search in the server: {}", url);
            return queryResponse;
        } catch (Exception e) {
            logger.error("Can't retrieve data from Solr: {}/select?{}", url, query, e);
            throw new RepositoryAccessException("Can't retrieve data from Solr: " + url, e);
        }
    }

    @Override
    public Set<String> getExistingIds(String idField,
                                      Set<String> ids) throws RepositoryAccessException {
        SolrQuery solrQuery = new SolrQuery(QueryParams.MATCH_ALL);
        String param = FQ.field(idField)
                .values(ids)
                .build();
        solrQuery.setFilterQueries(param);

        return getExistingIds(solrQuery, idField);
    }

    @Override
    public Set<String> getExistingIds(SolrQuery solrQuery,
                                      String idField,
                                      String route) {
        solrQuery.setFields(idField);

        try {
            QueryResponse queryResponse = solrClient.query(solrQuery);
            Set<String> ids = queryResponse.getResults().stream()
                    .map(d -> d.get(idField))
                    .map(v -> (String) v)
                    .collect(toSet());
            logger.info("Document count exists: {} for query: {}/select?{}", ids.size(), url, solrQuery);

            return ids;
        } catch (Exception e) {
            logger.error("Can't retrieve data from Solr: {}/select?{}", url, solrQuery, e);
            throw new RepositoryAccessException("Can't retrieve data from the Solr: " + url, e);
        }
    }

    @Override
    public Set<String> getExistingIds(SolrQuery solrQuery, String idField) throws RepositoryAccessException {
        return getExistingIds(solrQuery, idField, null);
    }

    @Override
    public boolean exists(String idField,
                          String id,
                          String route) {
        SolrQuery solrQuery = new SolrQuery(QueryParams.MATCH_ALL);
        String param = FQ.field(idField)
                .value(id)
                .build();
        solrQuery.setFilterQueries(param);

        return exists(solrQuery, idField, route);
    }

    @Override
    public boolean exists(String idField, String id) throws RepositoryAccessException {
        return exists(idField, id, null);
    }

    @Override
    public boolean exists(SolrQuery solrQuery,
                          String idField,
                          String route) {
        return !getExistingIds(solrQuery, idField, route).isEmpty();
    }

    @Override
    public boolean exists(SolrQuery solrQuery, String idField) throws RepositoryAccessException {
        return exists(solrQuery, idField, null);
    }

    @Override
    public void softCommit() {
        try {
            logger.info("Starting to commit: {}", url);
            UpdateRequest updateRequest = createRequest();

            updateRequest.setAction(COMMIT, false, false, softCommit).process(solrClient);
            logger.info("Documents have been successfully updated in Solr: {}", url);
        } catch (Exception e) {
            throw new RepositoryAccessException("Failed to commit.", e);
        }
    }

    private void save(SolrInputDoc doc,
                      boolean softCommit,
                      Integer commitWithin,
                      String route) {
        logger.info("Starting to add document in Solr: {}, document: {}, route: {}", url, doc, route);
        try {
            process(doc.getSolrInputDocument(), commitWithin);

            if (softCommit) {
                softCommit();
            }
            logger.info("Document have been successfully added in Solr: {}", url);
        } catch (Exception e) {
            logger.error("Can't save documents in Solr: {}", url);
            throw new RepositoryAccessException("Can't save document in Solr: " + url, e);
        }
    }

    private void process(List<SolrInputDocument> docs) throws IOException, SolrServerException {
        UpdateRequest updateRequest =
                createRequest()
                .add(docs);

        updateRequest.process(solrClient);
    }

    private void process(SolrInputDocument doc,
                         Integer commitWithin) throws SolrServerException, IOException {
        UpdateRequest updateRequest = createRequest()
                .add(doc, commitWithin);

        updateRequest.process(solrClient);
    }

    private UpdateRequest createRequest() {
        return new UpdateRequest();
    }
}