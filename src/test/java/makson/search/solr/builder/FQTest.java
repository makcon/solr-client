package makson.search.solr.builder;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static makson.search.solr.builder.FQ.and;
import static makson.search.solr.builder.FQ.or;
import static org.junit.Assert.assertEquals;

public class FQTest {

    private static final String STRING_FIELD = "StringField";
    private static final String DATE_FIELD = "DateField";
    private static final String STRING_FIELD_2 = "StringField2";
    private static final String BOOL_FIELD = "BoolField";
    private static final String STRING_FIELD_3 = "StringField3";

    @Test
    public void fieldIsTrue() {
        final String actual = FQ
                .field(STRING_FIELD)
                .isTrue()
                .build();

        assertEquals("StringField:true", actual);
    }

    @Test
    public void fieldIsFalse() {
        final String actual = FQ
                .field(STRING_FIELD)
                .isFalse()
                .build();

        assertEquals("StringField:false", actual);
    }

    @Test
    public void values_oneValue() {
        final String actual = FQ
                .field(STRING_FIELD)
                .values("value")
                .build();

        assertEquals("StringField:value", actual);
    }

    @Test
    public void values_twoValues() {
        final String actual = FQ
                .field(STRING_FIELD)
                .values("val1", "val2")
                .build();

        assertEquals("StringField:(val1 OR val2)", actual);
    }

    @Test
    public void values_oneValuesList() {
        final String actual = FQ
                .field(STRING_FIELD)
                .values(singletonList("val1"))
                .build();

        assertEquals("StringField:val1", actual);
    }

    @Test
    public void values_twoValuesList() {
        final String actual = FQ
                .field(STRING_FIELD)
                .values(asList("val1", "val2"))
                .build();

        assertEquals("StringField:(val1 OR val2)", actual);
    }

    @Test
    public void exclude_oneValue() {
        final String actual = FQ
                .excludeField(STRING_FIELD)
                .values("value1")
                .build();

        assertEquals("(*:* -StringField:value1)", actual);
    }

    @Test
    public void exclude_twoValues() {
        final String actual = FQ
                .excludeField(STRING_FIELD)
                .values("value1", "value2")
                .build();

        assertEquals("(*:* -StringField:(value1 OR value2))", actual);
    }

    @Test
    public void excludeIf_true() {
        final String actual = FQ
                .excludeFieldIf(true, STRING_FIELD)
                .values("value1")
                .build();

        assertEquals("(*:* -StringField:value1)", actual);
    }

    @Test
    public void excludeIf_false() {
        final String actual = FQ
                .excludeFieldIf(false, STRING_FIELD)
                .values("value1")
                .build();

        assertEquals("StringField:value1", actual);
    }

    @Test
    public void fieldWithPrefix() {
        final String actual = FQ
                .fieldWithPrefix("prefix", STRING_FIELD)
                .values("value1")
                .build();

        assertEquals("prefixStringField:value1", actual);
    }

    @Test
    public void range_fromTo() {
        String expected = "StringField:[1 TO 2]";

        final String actual = FQ
                .field(STRING_FIELD)
                .range(1, 2)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void range_fromOnly() {
        String expected = "StringField:[1 TO *]";

        final String actual = FQ
                .field(STRING_FIELD)
                .range(1, -1)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void range_toOnly() {
        String expected = "StringField:[* TO 2]";

        final String actual = FQ
                .field(STRING_FIELD)
                .range(-1, 2)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void range_noRange() {
        String expected = "StringField:[* TO *]";

        final String actual = FQ
                .field(STRING_FIELD)
                .range(-1, -1)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void absentField_() {
        final String actual = FQ
                .absentField(STRING_FIELD)
                .build();

        assertEquals("(*:* -StringField:[* TO *])", actual);
    }

    @Test
    public void withDateRange_twoParamsLocalDateTime() {
        String expected = "DateField:[1970-01-01T00:00:00.000Z TO 1970-01-01T23:59:59.999Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateRange(
                        Instant.ofEpochMilli(123).atZone(UTC).toLocalDateTime(),
                        Instant.ofEpochMilli(456).atZone(UTC).toLocalDateTime()
                )
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withDateRange_oneParamLong() {
        long[] dates = {123};

        String expected = "DateField:[1970-01-01T00:00:00.000Z TO 1970-01-01T23:59:59.999Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateRange(dates)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withDateRange_twoParamsLong() {
        long[] dates = {123456789, 123};

        String expected = "DateField:[1970-01-01T00:00:00.000Z TO 1970-01-02T23:59:59.999Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateRange(dates)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withDateRange_threeParamsLong() {
        long[] dates = {123456789, 923456789, 123};

        String expected = "DateField:[1970-01-01T00:00:00.000Z TO 1970-01-11T23:59:59.999Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateRange(dates)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withDateRange_oneDateLong() {
        long date = 123;

        String expected = "DateField:[1970-01-01T00:00:00.000Z TO 1970-01-01T23:59:59.999Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateRange(date)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withDateRange_oneDateLocalDateTime() {
        LocalDateTime date =  Instant.ofEpochMilli(123).atZone(UTC).toLocalDateTime();

        String expected = "DateField:[1970-01-01T00:00:00.000Z TO 1970-01-01T23:59:59.999Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateRange(date)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withDateRange_threeParamsSet() {
        Set<LocalDateTime> dates = new HashSet<>(asList(
                Instant.ofEpochMilli(123456789).atZone(UTC).toLocalDateTime(),
                Instant.ofEpochMilli(123).atZone(UTC).toLocalDateTime(),
                Instant.ofEpochMilli(123456).atZone(UTC).toLocalDateTime()
        ));

        String expected = "DateField:[1970-01-01T00:00:00.000Z TO 1970-01-02T23:59:59.999Z]";

        String actual = FQ
                .field(DATE_FIELD)
                .dateRange(dates)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withLocalDateTimeRange_twoParamsLocalDateTime() {
        String expected = "DateField:[1970-01-01T00:00:00.123Z TO 1970-01-01T00:00:00.456Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateTimeRange(
                        Instant.ofEpochMilli(123).atZone(UTC).toLocalDateTime(),
                        Instant.ofEpochMilli(456).atZone(UTC).toLocalDateTime()
                )
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withLocalDateTimeRange_oneParamLong() {
        long[] dates = {123};

        String expected = "DateField:[1970-01-01T00:00:00.123Z TO 1970-01-01T00:00:00.123Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateTimeRange(dates)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withLocalDateTimeRange_twoParamsLong() {
        long[] dates = {123456789, 123};

        String expected = "DateField:[1970-01-01T00:00:00.123Z TO 1970-01-02T10:17:36.789Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateTimeRange(dates)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withLocalDateTimeRange_threeParamsLong() {
        long[] dates = {123456789, 923456789, 123};

        String expected = "DateField:[1970-01-01T00:00:00.123Z TO 1970-01-11T16:30:56.789Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateTimeRange(dates)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withLocalDateTimeRange_oneDateLong() {
        long date = 123;

        String expected = "DateField:[1970-01-01T00:00:00.123Z TO 1970-01-01T00:00:00.123Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateTimeRange(date)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withLocalDateTimeRange_oneDateLocalDateTime() {
        LocalDateTime date = Instant.ofEpochMilli(123).atZone(UTC).toLocalDateTime();

        String expected = "DateField:[1970-01-01T00:00:00.123Z TO 1970-01-01T00:00:00.123Z]";

        final String actual = FQ
                .field(DATE_FIELD)
                .dateTimeRange(date)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void withLocalDateTimeRange_threeParamsSet() {
        Set<LocalDateTime> dates = new HashSet<>(asList(
                Instant.ofEpochMilli(123456789).atZone(UTC).toLocalDateTime(),
                Instant.ofEpochMilli(123).atZone(UTC).toLocalDateTime(),
                Instant.ofEpochMilli(123456).atZone(UTC).toLocalDateTime()
        ));
        String expected = "DateField:[1970-01-01T00:00:00.123Z TO 1970-01-02T10:17:36.789Z]";

        String actual = FQ
                .field(DATE_FIELD)
                .dateTimeRange(dates)
                .build();

        assertEquals(expected, actual);
    }

    @Test
    public void joinAnd() {
        String expected = "StringField:(val1 OR val2) AND (*:* -StringField2:US)";

        String actual = and(
                FQ.field(STRING_FIELD).values("val1", "val2"),
                FQ.excludeField(STRING_FIELD_2).value("US")
        ).build();

        assertEquals(expected, actual);
    }

    @Test
    public void joinOr() {
        String expected = "{prefix}StringField:val1 OR StringField2:US";

        String actual = or(
                FQ.fieldWithPrefix("{prefix}", STRING_FIELD).value("val1"),
                FQ.field(STRING_FIELD_2).value("US")
        ).build();

        assertEquals(expected, actual);
    }

    @Test
    public void joinAndCollection() {
        String expected = "StringField:(val1 OR val2) AND (*:* -StringField2:US)";

        List<FQ> clauses = asList(
                FQ.field(STRING_FIELD).values("val1", "val2"),
                FQ.excludeField(STRING_FIELD_2).value("US")
        );

        String actual = and(clauses).build();

        assertEquals(expected, actual);
    }

    @Test
    public void joinOrCollection() {
        String expected = "{prefix}StringField:val1 OR StringField2:US";

        List<FQ> clauses = asList(
                FQ.fieldWithPrefix("{prefix}", STRING_FIELD).value("val1"),
                FQ.field(STRING_FIELD_2).value("US")
        );

        String actual = or(clauses).build();

        assertEquals(expected, actual);
    }

    @Test
    public void joinAnd_emptyClause() {
        String expected = "StringField:val1";

        String actual = and(
                FQ.field(STRING_FIELD).values("val1"),
                FQ.empty()
        ).build();

        assertEquals(expected, actual);
    }

    @Test
    public void join_singleNestedClause_noParenthesis() {
        String expected = "(StringField:val1 AND StringField2:US) OR BoolField:true";

        String actual = or(
                and(
                        FQ.field(STRING_FIELD).values("val1"),
                        FQ.field(STRING_FIELD_2).value("US")
                ),
                or(
                        FQ.field(BOOL_FIELD).isTrue(),
                        FQ.empty()
                )
        ).build();

        assertEquals(expected, actual);
    }

    @Test
    public void joinOr_oneValue_noParenthesis() {
        String expected = "StringField:val1";

        String actual = or(
                FQ.field(STRING_FIELD).values("val1")
        ).build();

        assertEquals(expected, actual);
    }

    @Test
    public void joinOr_oneANDClauseInsideOR_noParenthesis() {
        String expected = "StringField:val1 AND StringField2:US";

        FQ params = and(
                FQ.field(STRING_FIELD).values("val1"),
                FQ.field(STRING_FIELD_2).value("US")
        );

        String actual = or(params).build();

        assertEquals(expected, actual);
    }

    @Test
    public void buildComplexParam() {
        String expected = "(StringField:(val1 OR val2) AND (*:* -StringField2:US) " +
                "AND BoolField:true) OR (StringField3:RU OR (*:* -StringField3:[* TO *]))";

        String actual = or(
                and(
                        FQ.field(STRING_FIELD).values("val1", "val2"),
                        FQ.excludeField(STRING_FIELD_2).value("US"),
                        FQ.field(BOOL_FIELD).isTrue()
                ),
                or(
                        FQ.field(STRING_FIELD_3).value("RU"),
                        FQ.absentField(STRING_FIELD_3)
                )
        ).build();

        assertEquals(expected, actual);
    }
}