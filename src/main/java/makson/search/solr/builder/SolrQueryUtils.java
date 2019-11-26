package makson.search.solr.builder;

import lombok.experimental.UtilityClass;
import org.apache.solr.client.solrj.SolrQuery;

@UtilityClass
public class SolrQueryUtils {

    public SolrQuery getSolrQueryCopy(SolrQuery solrQuery) {
        SolrQuery copy = solrQuery.getCopy();
        copy.setSorts(solrQuery.getSorts());
        return copy;
    }
}