package makcon.solr.client.dataobject;

import makcon.solr.client.constant.SolrFieldModifier;
import org.apache.solr.common.SolrInputDocument;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SolrInputDoc {

    // TODO add tests

    private final SolrInputDocument solrInputDocument;

    private SolrInputDoc(SolrInputDocument solrInputDocument) {
        this.solrInputDocument = solrInputDocument;
    }

    public SolrInputDocument getSolrInputDocument() {
        return solrInputDocument;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String field) {
        Object value = solrInputDocument.getFieldValue(field);
        if (value instanceof Map) {
            value = getModifiedValue(field, value);
        }

        return (T) value;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getList(String field) {
        Object value = solrInputDocument.getFieldValues(field);
        if (value instanceof Map) {
            value = getModifiedValue(field, value);
        }

        return (List<T>) value;
    }

    @SuppressWarnings("unchecked")
    public <T> T getAtomic(String field,
                           SolrFieldModifier modifier) {
        Object value = solrInputDocument.getFieldValue(field);
        if (value instanceof Map) {
            Map<String, Object> fieldValue = (Map<String, Object>) value;
            Object atomicValue = fieldValue.get(modifier.value);
            if (atomicValue == null) {
                throw new RuntimeException("Atomic value not found for: " + value);
            }
            return (T) atomicValue;
        }

        throw new UnsupportedOperationException("The field is not for atomic update");
    }

    public Object getRaw(String field) {
        return solrInputDocument.getFieldValue(field);
    }

    public static Builder builder(boolean newDoc,
                                  String idField,
                                  Object id) {
        return new Builder(newDoc, idField, id);
    }

    public static Builder newDoc(String idField,
                                 Object id) {
        return new Builder(true, idField, id);
    }

    public static Builder toUpdate(String idField,
                                   Object id) {
        return new Builder(false, idField, id);
    }

    @Override
    public String toString() {
        return "SolrInputDoc{" + getFieldsToPrint() + '}';
    }

    @SuppressWarnings("unchecked")
    private Object getModifiedValue(String field,
                                    Object value) {
        Map<String, Object> fieldValue = (Map<String, Object>) value;
        Object val = null;
        for (SolrFieldModifier modifier : SolrFieldModifier.values()) {
            val = fieldValue.get(modifier.value);
            if (val != null) {
                break;
            }
        }

        if (val == null) {
            throw new RuntimeException(
                    "Can't find value for field: " + field + " in SolrDoc: " + solrInputDocument
            );
        }

        return val;
    }

    private String getFieldsToPrint() {
        StringBuilder sb = new StringBuilder();
        solrInputDocument.forEach((k, v) -> {
            sb.append(k);
            sb.append('=');
            Object value = v.getValue();
            if (value instanceof String) {
                String valueString = (String) value;
                int toIndex = valueString.length() < 30 ? valueString.length() - 1 : 29;
                value = valueString.substring(0, toIndex);
            }
            sb.append(value);
            sb.append(';');
        });

        return sb.toString();
    }

    public static final class Builder {

        private final SolrInputDocument solrInputDocument = new SolrInputDocument();
        private final Map<String, Object> fieldsMap = new HashMap<>();
        private final Map<String, Map<String, Object>> fieldsToUpdateMap = new HashMap<>();
        private final boolean newDoc;

        private Builder(boolean newDoc,
                        String idField,
                        Object id) {
            this.newDoc = newDoc;
            solrInputDocument.setField(idField, id);
        }

        public Builder setField(String solrField,
                                Object value) {
            if (newDoc) {
                fieldsMap.put(solrField, value);
            } else if (value != null) {
                fieldsToUpdateMap.put(solrField, addFieldModifier(value, SolrFieldModifier.SET));
            }

            return this;
        }

        public Builder addField(String solrField,
                                Object value) {
            if (newDoc) {
                throw new UnsupportedOperationException("Use setField method for new document");
            }
            fieldsToUpdateMap.put(solrField, addFieldModifier(value, SolrFieldModifier.ADD));

            return this;
        }

        public SolrInputDoc build() {
            if (newDoc) {
                buildForNew();
            } else {
                buildForUpdate();
            }

            return new SolrInputDoc(solrInputDocument);
        }

        private void buildForNew() {
            fieldsMap.forEach(solrInputDocument::setField);
        }

        private void buildForUpdate() {
            fieldsToUpdateMap.forEach(solrInputDocument::addField);
        }

        private Map<String, Object> addFieldModifier(Object value,
                                                     SolrFieldModifier modifier) {
                /*
                 * In the case of multi-valued fields if null is specified on set all
                 * the values in the field are removed.
                 * https://issues.apache.org/jira/browse/SOLR-3862
                 */
            if (value instanceof Collection && ((Collection<?>) value).isEmpty()) {
                value = null;
            }
            Map<String, Object> fieldModifier = new HashMap<>(1);
            fieldModifier.put(modifier.value, value);

            return fieldModifier;
        }
    }
}
