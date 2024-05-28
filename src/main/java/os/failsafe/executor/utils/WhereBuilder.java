package os.failsafe.executor.utils;

import os.failsafe.executor.Sort;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

public class WhereBuilder {

    private final Database database;
    private StringBuilder sb = new StringBuilder();
    private List<Object> params = new ArrayList<>();

    public WhereBuilder(Database database) {
        this.database = database;
    }

    public WhereBuilder where(String value, String field) {
        if (StringUtils.isBlank(value)) {
            return this;
        }

        if (sb.length() > 0) {
            sb.append(" AND ");
        } else {
            sb.append(" WHERE ");
        }

        sb.append(String.format("(%s = ?)", field));
        params.add(value);
        return this;
    }

    public WhereBuilder gte(LocalDateTime value, String field) {
        if (value == null) {
            return this;
        }

        Timestamp date = Timestamp.valueOf(value);

        if (sb.length() > 0) {
            sb.append(" AND ");
        } else {
            sb.append(" WHERE ");
        }

        sb.append(String.format("(%s >= ?)", field));
        params.add(date);
        return this;
    }

    public WhereBuilder lt(LocalDateTime value, String field) {
        if (value == null) {
            return this;
        }

        Timestamp date = Timestamp.valueOf(value);

        if (sb.length() > 0) {
            sb.append(" AND ");
        } else {
            sb.append(" WHERE ");
        }

        sb.append(String.format("(%s < ?)", field));
        params.add(date);
        return this;
    }

    public WhereBuilder containsIgnoreCase(String value, String field) {
        if (StringUtils.isBlank(value)) {
            return this;
        }

        if (sb.length() > 0) {
            sb.append(" AND ");
        } else {
            sb.append(" WHERE ");
        }

        sb.append(String.format("(LOWER(%s) LIKE ?)", field.toLowerCase()));
        params.add(value);
        return this;
    }

    public WhereBuilder isNullOrNotNull(Boolean notNull, String field) {
        if (notNull == null) {
            return this;
        }

        if (sb.length() > 0) {
            sb.append(" AND ");
        } else {
            sb.append(" WHERE ");
        }

        if (Boolean.TRUE.equals(notNull)) {
            sb.append(String.format("(%s IS NOT NULL)", field));
        } else {
            sb.append(String.format("(%s IS NULL)", field));
        }
        return this;
    }

    public WhereBuilder orderBy(Sort... sorts) {
        String orderBys = Arrays.stream(sorts).map(Sort::toString).collect(Collectors.joining(","));

        sb.append(" ORDER BY ");
        sb.append(orderBys);
        return this;
    }

    public WhereBuilder limit(int offset, int limit) {
        if(database.isMysqlOrMariaDb()) {
            sb.append(" LIMIT ?, ?");
        } else {
            sb.append(" OFFSET ? ROWS FETCH NEXT ? ROWS ONLY");
        }

        params.add(offset);
        params.add(limit);
        return this;
    }

    public WhereClause build() {
        return new WhereClause(sb.toString(), params.toArray());
    }

    public static class WhereClause {
        public final String where;
        public final Object[] params;

        public WhereClause(String where, Object[] params) {
            this.where = where;
            this.params = params;
        }
    }
}
