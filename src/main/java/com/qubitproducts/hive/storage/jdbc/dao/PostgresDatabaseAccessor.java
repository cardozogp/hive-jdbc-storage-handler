package com.qubitproducts.hive.storage.jdbc.dao;

/**
 * Postgres specific data accessor. This is needed because Postgres JDBC drivers do not support generic LIMIT and OFFSET
 * escape functions
 */
public class PostgresDatabaseAccessor extends GenericJdbcDatabaseAccessor {
	
	@Override
    protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
        if (offset == 0) {
            return addLimitToQuery(sql, limit);
        }
        else {
            return sql + " LIMIT " + limit + "," + offset;
        }
    }


    @Override
    protected String addLimitToQuery(String sql, int limit) {
        return sql + " LIMIT " + limit;
    }

}
