package makson.search.solr.repository;

import makson.search.solr.dataobject.SolrDoc;
import makson.search.solr.dataobject.SolrInputDoc;
import makson.search.solr.exception.RepositoryAccessException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface SolrRepository {

    void save(SolrInputDoc document) throws RepositoryAccessException;

    void save(SolrInputDoc document,
              Integer commitWithin) throws RepositoryAccessException;

    void save(SolrInputDoc document,
              Integer commitWithin,
              String route) throws RepositoryAccessException;

    void save(SolrInputDoc document,
              boolean softCommit) throws RepositoryAccessException;

    void save(SolrInputDoc document,
              boolean softCommit,
              String route) throws RepositoryAccessException;

    void save(List<SolrInputDoc> document) throws RepositoryAccessException;

    void deleteById(String id) throws RepositoryAccessException;

    void deleteByIds(List<String> ids) throws RepositoryAccessException;

    void deleteByQuery(String query) throws RepositoryAccessException;

    Optional<SolrDoc> find(SolrQuery query) throws RepositoryAccessException;

    List<SolrDoc> findList(SolrQuery query) throws RepositoryAccessException;

    QueryResponse findByQuery(SolrQuery query) throws RepositoryAccessException;

    Set<String> getExistingIds(String idField,
                               Set<String> ids) throws RepositoryAccessException;

    Set<String> getExistingIds(SolrQuery solrQuery,
                               String idField) throws RepositoryAccessException;

    Set<String> getExistingIds(SolrQuery solrQuery,
                               String idField,
                               String route) throws RepositoryAccessException;

    boolean exists(String idField,
                   String id) throws RepositoryAccessException;

    boolean exists(String idField,
                   String id,
                   String route) throws RepositoryAccessException;

    boolean exists(SolrQuery solrQuery,
                   String idField) throws RepositoryAccessException;

    boolean exists(SolrQuery solrQuery,
                   String idField,
                   String route) throws RepositoryAccessException;

    void softCommit();

    void shutdown();
}