/*
 * Copyright (c) 2018 by Philippe Marschall <philippe.marschall@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ferstl.spring.jdbc.oracle;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Arrays;

import org.springframework.dao.CleanupFailureDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.core.SqlTypeValue;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;

/**
 * An implementation of {@link SqlTypeValue} for the convenient creation
 * of an Oracle {@link Array} from the provided composite values.
 *
 * <h2>SQL Syntax</h2>
 * Any of the following two syntax can be used to filter columns against an array of composite values
 * <pre>
 * <code>WHERE (filtered_column_1, filtered_column_2) = ANY(SELECT * FROM table(:ids))</code>
 * </pre>
 * <pre>
 * <code>WHERE (filtered_column_1, filtered_column_2) IN (SELECT attribute_1, attribute_2 FROM table(:ids))</code>
 * </pre>
 * {@code filtered_column_x} has to be replaced with the name of the column to filter against
 * the array. {@code attribute_x} has to be replaced with the name of attribute of the composite type.
 * <p>{@code *} or listing the attributes can be used with either the {@code IN} or the {@code ANY}
 * syntax.
 * 
 * <h2>JdbcTemplate Example</h2>
 * <pre><code> jdbcTemplate.queryForInt("SELECT val "
 *          + "FROM test_table "
 *          + "WHERE (id, val) = ANY(SELECT * FROM table(?))",
 *             new SqlOracleArrayOfStructValue("MYARRAYTYPE", "MYCOMPOSITETYPE", values));</code></pre>
 *
 * <h2>OracleNamedParameterJdbcTemplate Example</h2>
 * <pre><code> Map&lt;String, Object&gt; map = Collections.singletonMap("ids", new SqlOracleArrayOfStructValue("MYARRAYTYPE", "MYCOMPOSITETYPE", values));
 * namedParameterJdbcTemplate.query("SELECT val "
 *          + "FROM test_table "
 *          + "WHERE (id, val) = ANY(SELECT * FROM table(:ids))",
 *             new MapSqlParameterSource(map),
 *            (rs, i) -&gt; ...);
 * </code></pre>
 *
 * <h2>StoredProcedure Example</h2>
 * <pre><code> storedProcedure.declareParameter(new SqlParameter("myarrayparameter", Types.ARRAY, "MYARRAYTYPE"));
 * ...
 * Map&lt;String, Object&gt; inParams = new HashMap&lt;&gt;();
 * inParams.put("myarrayparameter", new SqlOracleArrayOfStructValue("MYARRAYTYPE", "MYCOMPOSITETYPE", objectArray);
 * Map&lt;String, Object&gt; out = storedProcedure.execute(inParams);
 * </code></pre>
 *
 *
 * <p>This class is similar to {@code org.springframework.data.jdbc.support.oracle.SqlArrayValue}
 * but updated for Spring 5 and later and OJDBC 11.2g and later.
 *
 * <p>This class can be combined with {@link OracleNamedParameterJdbcTemplate} for named parameter
 * support.
 *
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html#createOracleArray_java_lang_String_java_lang_Object_">OracleConnection#createOracleArray</a>
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/lnpls/plsql-collections-and-records.html#GUID-5ADB7EE2-71F6-4172-ACD8-FFDCF2787A37">6.4 Nested Tables </a>
 * @see Connection#createStruct(String, Object[])
 */
public final class SqlOracleArrayOfStructValue implements NamedSqlValue {

  private final Object[][] values;

  private final String arrayTypeName;

  private final String structTypeName;

  private Array array;

  /**
   * Constructor that takes two parameters, one parameter with the array of values passed in to
   * the statement and one that takes the type name.
   *
   * @param arrayTypeName the name of the collection type
   * @param structTypeName the name of the collection component type
   * @param values the array containing the values
   */
  public SqlOracleArrayOfStructValue(String arrayTypeName, String structTypeName, Object[][] values) {
    this.arrayTypeName = arrayTypeName;
    this.structTypeName = structTypeName;
    this.values = values;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setValue(PreparedStatement ps, int paramIndex) throws SQLException {
    Array array = this.createArray(ps.getConnection());
    ps.setArray(paramIndex, array);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setValue(PreparedStatement ps, String paramName) throws SQLException {
    Array array = this.createArray(ps.getConnection());
    ps.unwrap(OraclePreparedStatement.class).setArrayAtName(paramName, array);
  }

  private Array createArray(Connection conn) throws SQLException {
    if (this.array != null) {
      throw new InvalidDataAccessApiUsageException("Value bound more than once");
    }
    Struct[] structs = new Struct[this.values.length];
    for (int i = 0; i < this.values.length; i++) {
      Object[] structValues = this.values[i];
      Struct struct = conn.createStruct(this.structTypeName, structValues);
      structs[i] = struct;
    }
    this.array = conn.unwrap(OracleConnection.class).createOracleArray(this.arrayTypeName, structs);
    return this.array;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void cleanup() {
    if (this.array == null) {
      // #cleanup may be called twice in case of exceptions
      // avoid calling #free twice
      return;
    }
    // https://docs.oracle.com/javase/tutorial/jdbc/basics/array.html#releasing_array
    try {
      this.array.free();
      this.array = null;
    } catch (SQLException e) {
      throw new CleanupFailureDataAccessException("could not free array", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return Arrays.toString(this.values);
  }

}
