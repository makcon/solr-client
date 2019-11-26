package makson.search.solr.dataobject;

import makson.search.solr.exception.FieldNotFoundException;
import org.apache.solr.common.SolrDocument;

import javax.annotation.Nonnull;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class SolrDoc {

    private final SolrDocument solrDocument;

    public SolrDoc(SolrDocument solrDocument) {
        this.solrDocument = solrDocument;
    }

    public SolrDocument getSolrDocument() {
        return solrDocument;
    }

    public <T> Optional<T> get(String solrField,
                               Class<T> type) {
        return Optional.ofNullable(type.cast(getObject(solrField)));
    }

    public Optional<String> getString(String solrField) {
        return get(solrField, String.class);
    }

    public Optional<Integer> getInt(String solrField) {
        return get(solrField, Integer.class);
    }

    public Optional<Date> getDate(String solrField) {
        return get(solrField, Date.class);
    }

    public Optional<Boolean> getBool(String solrField) {
        return get(solrField, Boolean.class);
    }

    @Nonnull
    public <T> T getRequired(String solrField,
                             Class<T> type) {
        Object value = getObject(solrField);

        if (value == null) {
            throw new FieldNotFoundException(
                    "Field: " + solrField + " not found for document: " + solrDocument
            );
        }

        return type.cast(value);
    }

    @Nonnull
    public String getStringRequired(String solrField) {
        return getRequired(solrField, String.class);
    }

    @Nonnull
    public Date getDateRequired(String solrField) {
        return getRequired(solrField, Date.class);
    }

    @Nonnull
    public Integer getIntRequired(String solrField) {
        return getRequired(solrField, Integer.class);
    }

    @Nonnull
    public Boolean getBoolRequired(String solrField) {
        return getRequired(solrField, Boolean.class);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(String solrField,
                               Class<T> type) {
        Object object = getObject(solrField);
        return object == null ? emptyList() : (List<T>) object;
    }

    @Nonnull
    public List<String> getList(String solrField) {
        return getList(solrField, String.class);
    }

    @Nonnull
    public List<SolrDoc> getSubList(String solrField) {
        List<SolrDocument> documents = getList(solrField, SolrDocument.class);

        if (documents.isEmpty()) {
            return emptyList();
        }

        return documents.stream()
                .map(SolrDoc::new)
                .collect(toList());
    }

    private Object getObject(String solrField) {
        return solrDocument.get(solrField);
    }
}