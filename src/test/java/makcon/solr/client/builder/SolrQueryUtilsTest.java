package makcon.solr.client.builder;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Test;

import java.util.List;

import static makcon.solr.client.builder.SolrQueryUtils.getSolrQueryCopy;
import static org.apache.solr.client.solrj.SolrQuery.ORDER.asc;
import static org.junit.Assert.assertEquals;

public class SolrQueryUtilsTest {

    @Test
    public void getSolrQueryCopy_checkSortCopy() {
        SolrQuery solrQuery = new SolrQuery()
                .setSort("field", asc);

        SolrQuery solrQueryCopy = getSolrQueryCopy(solrQuery);

        List<SolrQuery.SortClause> sorts = solrQueryCopy.getSorts();
        assertEquals(1, sorts.size());
        SolrQuery.SortClause sortClause = sorts.get(0);
        assertEquals("field", sortClause.getItem());
        assertEquals(asc, sortClause.getOrder());
    }
}