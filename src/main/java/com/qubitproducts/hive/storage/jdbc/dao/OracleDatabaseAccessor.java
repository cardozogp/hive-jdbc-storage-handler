package com.qubitproducts.hive.storage.jdbc.dao;

/**
 * Created by srini on 6/3/15.



 * oracle specific data accessor.
 *
 */
public class OracleDatabaseAccessor extends GenericJdbcDatabaseAccessor {

    @Override
    protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {

            return addLimitToQuery(sql, limit);

    }


    @Override
    protected String addLimitToQuery(String sql, int limit) {
        return sql + " where rownum<= " + limit;
    }

}
