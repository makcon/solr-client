package makcon.solr.client.builder;

import lombok.RequiredArgsConstructor;
import makcon.solr.client.constant.QueryParams;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.joining;
import static makcon.solr.client.builder.FQ.JoinOperator.AND;
import static makcon.solr.client.builder.FQ.JoinOperator.OR;

public final class FQ {

    @RequiredArgsConstructor
    enum JoinOperator {

        OR(" OR "),
        AND(" AND ");

        private final String value;
    }

    private static final String DATE_RANGE_PATTERN = "[%sT00:00:00.000Z TO %sT23:59:59.999Z]";
    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN).withZone(ZoneId.systemDefault());

    private static final String DATE_TIME_RANGE_PATTERN = "[%s TO %s]";
    private static final String DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN).withZone(ZoneId.systemDefault());

    private static final char PARENTHESES_OPEN = '(';
    private static final char PARENTHESES_CLOSE = ')';

    private String param;

    private String field;
    private String value;
    private boolean exclude;
    private String prefix;

    private FQ[] clauses;
    private JoinOperator joinOperator;
    private boolean joinParenthesis = true;

    private boolean empty;

    private FQ() {
    }

    public static FQ empty() {
        return create().setEmpty(true);
    }

    public static FQ fieldWithPrefix(@Nonnull String prefix,
                                     @Nonnull String field) {
        return new FQ()
                .setPrefix(prefix)
                .setField(field);
    }

    public static FQ field(@Nonnull String field) {
        return new FQ().setField(field);
    }

    public static FQ excludeField(@Nonnull String field) {
        return new FQ()
                .setExclude(true)
                .setField(field);
    }

    public static FQ excludeFieldIf(boolean exclude,
                                    @Nonnull String field) {
        return new FQ()
                .setExclude(exclude)
                .setField(field);
    }

    public FQ isTrue() {
        return setValue("true");
    }

    public FQ isFalse() {
        return setValue("false");
    }

    public static FQ absentField(@Nonnull String field) {
        return new FQ()
                .setExclude(true)
                .setField(field)
                .setValue("[* TO *]");
    }

    public FQ range(long from, long to) {
        return setValue(
                '[' +
                buildRangeItem(from) +
                " TO " +
                buildRangeItem(to) +
                ']'
        );
    }

    public FQ dateRange(long from,
                        long to) {
        return setValue(toSolrRange(from, to, false));
    }

    public FQ dateRange(long date) {
        return dateRange(date, date);
    }

    public FQ dateRange(@Nonnull LocalDateTime date) {
        return dateRange(date, date);
    }

    public FQ dateRange(@Nonnull LocalDateTime from,
                        @Nonnull LocalDateTime to) {
        return setValue(toSolrRange(from, to, false));
    }

    public FQ dateRange(@Nonnull Collection<LocalDateTime> dates) {
        return dateRange(dates, false);
    }

    public FQ dateRange(long[] dates) {
        final long min = Arrays.stream(dates)
                .min()
                .orElseThrow(() -> new IllegalArgumentException("Min value not found"));
        final long max = Arrays.stream(dates)
                .max()
                .orElseThrow(() -> new IllegalArgumentException("Max value not found"));

        return setValue(toSolrRange(min, max, false));
    }

    public FQ dateTimeRange(long from,
                            long to) {
        return setValue(toSolrRange(from, to, true));
    }

    public FQ dateTimeRange(long date) {
        return dateTimeRange(date, date);
    }

    public FQ dateTimeRange(@Nonnull LocalDateTime date) {
        return dateTimeRange(date, date);
    }

    public FQ dateTimeRange(@Nonnull LocalDateTime from,
                            @Nonnull LocalDateTime to) {
        return setValue(toSolrRange(from, to, true));
    }

    public FQ dateTimeRange(@Nonnull Collection<LocalDateTime> dates) {
        return dateRange(dates, true);
    }

    public FQ dateTimeRange(long[] dates) {
        final long min = Arrays.stream(dates)
                .min()
                .orElseThrow(() -> new IllegalArgumentException("Min value not found"));
        final long max = Arrays.stream(dates)
                .max()
                .orElseThrow(() -> new IllegalArgumentException("Max value not found"));

        return setValue(toSolrRange(min, max, true));
    }

    public FQ value(@Nonnull String value) {
        return setValue(value);
    }

    public FQ value(int value) {
        return setValue(String.valueOf(value));
    }

    public FQ values(@Nonnull Collection<?> values) {
        return joinValues(OR, true, values);
    }

    public FQ values(@Nonnull String... values) {
        if (values.length == 1) {
            return setValue(values[0]);
        }

        Set<String> setValues = new LinkedHashSet<>();
        Collections.addAll(setValues, values);

        return joinValues(OR, true, setValues);
    }

    public static FQ or(@Nonnull FQ... clauses) {
        return joinClauses(OR, clauses);
    }

    public static FQ and(@Nonnull FQ... clauses) {
        return joinClauses(AND, clauses);
    }

    public static FQ or(@Nonnull Collection<FQ> clauses) {
        return joinClauses(OR, clauses);
    }

    public static FQ and(@Nonnull Collection<FQ> clauses) {
        return joinClauses(AND, clauses);
    }

    public String build() {
        if (param == null) {
            joinParenthesis = false;
            StringBuilder sb = new StringBuilder();
            buildParam(sb, this);
            param = sb.toString();
        }

        return param;
    }

    private static FQ joinClauses(JoinOperator joinOperator,
                                  FQ... clauses) {
        if (clauses.length == 1) {
            FQ clause = clauses[0];
            return create()
                    .setExclude(clause.exclude)
                    .setPrefix(clause.prefix)
                    .setField(clause.field)
                    .setValue(clause.value)
                    .setClauses(clause.clauses)
                    .setJoinOperator(clause.joinOperator);
        }

        int nonEmptyCount = getNonEmptyCount(clauses);

        return create()
                .setClauses(clauses)
                .setJoinOperator(joinOperator)
                .setJoinParenthesis(nonEmptyCount > 1);
    }

    private static int getNonEmptyCount(FQ[] clauses) {
        int nonEmptyCount = 0;
        for (FQ clause : clauses) {
            if (!clause.empty) {
                nonEmptyCount++;
            }
        }
        return nonEmptyCount;
    }

    private static FQ joinClauses(JoinOperator joinOperator,
                                  Collection<FQ> clauses) {
        if (clauses.isEmpty()) {
            return empty();
        }

        return joinClauses(
                joinOperator,
                clauses.toArray(new FQ[]{})
        );
    }

    private FQ joinValues(JoinOperator operator,
                          boolean parentheses,
                          Collection<?> values) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Values can not be empty");
        }
        if (values.size() == 1) {
            return setValue(String.valueOf(values.iterator().next()));
        }

        String param = values.stream()
                .map(String::valueOf)
                .collect(joining(operator.value));

        param = parentheses ?
                PARENTHESES_OPEN + param + PARENTHESES_CLOSE :
                param;

        return setValue(param);
    }

    private void buildParam(StringBuilder sb,
                            FQ fq) {
        if (fq.prefix != null) {
            sb.append(fq.prefix);
        }
        if (fq.exclude) {
            appendExcludedPrefix(sb);
        }
        if (fq.field != null) {
            sb.append(fq.field);
            sb.append(':');
            sb.append(fq.value);
        }
        if (fq.exclude) {
            sb.append(PARENTHESES_CLOSE);
        }
        if (fq.clauses != null) {
            appendClauses(sb, fq);
        }
    }

    private void appendClauses(StringBuilder sb, FQ fq) {
        if (fq.joinParenthesis) {
            sb.append(PARENTHESES_OPEN);
        }
        for (int i = 0; i < fq.clauses.length; i++) {
            if (!fq.clauses[i].empty) {
                if (i != 0) {
                    sb.append(fq.joinOperator.value);
                }
                buildParam(sb, fq.clauses[i]);
            }
        }
        if (fq.joinParenthesis) {
            sb.append(PARENTHESES_CLOSE);
        }
    }

    private void appendExcludedPrefix(StringBuilder sb) {
        sb.append(PARENTHESES_OPEN);
        sb.append(QueryParams.MATCH_ALL);
        sb.append(' ');
        sb.append('-');
    }

    private FQ setValue(String value) {
        this.value = value;
        return this;
    }

    private String buildRangeItem(long value) {
        return String.valueOf(value == -1 ? "*" : value);
    }

    private FQ dateRange(@Nonnull Collection<LocalDateTime> dates,
                         boolean useClientTime) {
        if (dates.isEmpty()) {
            throw new IllegalArgumentException("Dates can not be empty");
        }

        final LocalDateTime min = dates.stream()
                .min(Comparator.naturalOrder())
                .orElseThrow(() -> new IllegalArgumentException("Min value not found"));
        final LocalDateTime max = dates.stream()
                .max(Comparator.naturalOrder())
                .orElseThrow(() -> new IllegalArgumentException("Max value not found"));

        return setValue(toSolrRange(min, max, useClientTime));
    }

    private String toSolrRange(LocalDateTime from,
                               LocalDateTime to,
                               boolean useClientTime) {
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("End date must be greater than start date");
        }


        final DateTimeFormatter formatter;
        final String pattern;
        if (useClientTime) {
            formatter = DATE_TIME_FORMATTER;
            pattern = DATE_TIME_RANGE_PATTERN;
        } else {
            formatter = DATE_FORMATTER;
            pattern = DATE_RANGE_PATTERN;
        }

        String fromAsString = from.format(formatter);
        String toAsString = to.format(formatter);

        return String.format(pattern, fromAsString, toAsString);
    }

    private String toSolrRange(long from,
                               long to,
                               boolean useClientTime) {
        return toSolrRange(
                Instant.ofEpochMilli(from).atZone(UTC).toLocalDateTime(),
                Instant.ofEpochMilli(to).atZone(UTC).toLocalDateTime(),
                useClientTime
        );
    }

    private static FQ create() {
        return new FQ();
    }

    private FQ setField(String field) {
        this.field = field;
        return this;
    }

    private FQ setExclude(boolean exclude) {
        this.exclude = exclude;
        return this;
    }

    private FQ setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    private FQ setEmpty(boolean empty) {
        this.empty = empty;
        return this;
    }

    private FQ setClauses(FQ[] clauses) {
        this.clauses = clauses;
        return this;
    }

    private FQ setJoinOperator(JoinOperator joinOperator) {
        this.joinOperator = joinOperator;
        return this;
    }

    private FQ setJoinParenthesis(boolean joinParenthesis) {
        this.joinParenthesis = joinParenthesis;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FQ fq = (FQ) o;

        if (exclude != fq.exclude) return false;
        if (!Objects.equals(param, fq.param)) return false;
        if (!field.equals(fq.field)) return false;
        if (!Objects.equals(value, fq.value)) return false;
        if (!Objects.equals(prefix, fq.prefix)) return false;
        if (!Arrays.equals(clauses, fq.clauses)) return false;
        return joinOperator == fq.joinOperator;

    }

    @Override
    public int hashCode() {
        int result = param != null ? param.hashCode() : 0;
        result = 31 * result + (field != null ? field.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (exclude ? 1 : 0);
        result = 31 * result + (prefix != null ? prefix.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(clauses);
        result = 31 * result + (joinOperator != null ? joinOperator.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FQ{" +
                "field=" + field +
                ", value='" + value + '\'' +
                ", clauses=" + Arrays.toString(clauses) +
                ", joinOperator=" + joinOperator +
                ", joinParenthesis=" + joinParenthesis +
                '}';
    }
}