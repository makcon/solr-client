package makson.search.solr.constant;

public enum SolrFieldModifier {

    /**
     * Adds an additional value to a list.
     */
    ADD("add"),
    /**
     * Set or replace a particular value,
     * or remove the value if null is specified as the new value.
     */
    SET("set"),
    /**
     * Removes a value (or a list of values) from a list.
     */
    REMOVE("remove"),
    /**
     * Removes from a list that match the given Java regular expression.
     */
    REMOVE_REGEX("removeregex"),
    /**
     * Increments a numeric value by a specific amount
     * (use a negative value to decrement).
     */
    INC("inc");

    public final String value;

    SolrFieldModifier(String value) {
        this.value = value;
    }
}