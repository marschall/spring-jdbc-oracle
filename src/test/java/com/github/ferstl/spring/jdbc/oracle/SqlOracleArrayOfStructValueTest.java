package com.github.ferstl.spring.jdbc.oracle;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Struct;

import org.junit.jupiter.api.Test;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;

public class SqlOracleArrayOfStructValueTest {

  @Test
  public void execution() throws SQLException {
    Object[][] values = new Object[][] {{1L, "value1"}, {2L, "value2"}};
    String arrayTypeName = "ARRAY_TYPE";
    String structTypeName = "STRUCT_TYPE";
    String paramName = "parameter1";
    NamedSqlValue value = new SqlOracleArrayOfStructValue(arrayTypeName, structTypeName, values);

    Connection connection = mock(Connection.class);
    OracleConnection oracleConnection = mock(OracleConnection.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    OraclePreparedStatement oraclePreparedStatement = mock(OraclePreparedStatement.class);
    Array array = mock(Array.class);
    Struct struct1 = mock(Struct.class);
    Struct struct2 = mock(Struct.class);

    when(connection.unwrap(OracleConnection.class)).thenReturn(oracleConnection);
    when(preparedStatement.unwrap(OraclePreparedStatement.class)).thenReturn(oraclePreparedStatement);
    when(preparedStatement.getConnection()).thenReturn(connection);

    when(oracleConnection.createOracleArray(arrayTypeName, new Object[] {struct1, struct2})).thenReturn(array);
    when(connection.createStruct(structTypeName, values[0])).thenReturn(struct1);
    when(connection.createStruct(structTypeName, values[1])).thenReturn(struct2);

    value.setValue(preparedStatement, paramName);

    verify(oraclePreparedStatement).setArrayAtName(paramName, array);
    verify(array, never()).free();

    value.cleanup();

    verify(array).free();

  }

}
