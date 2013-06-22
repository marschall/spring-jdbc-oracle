package com.github.ferstl.spring.jdbc.oracle;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.ParameterDisposer;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;

public class TestBatchPreparedStatementSetter implements BatchPreparedStatementSetter, ParameterDisposer {

  private int[] parameters;

  public TestBatchPreparedStatementSetter(int numberOfRows) {
    this.parameters = new int[numberOfRows];

    for (int i = 0; i < numberOfRows; i++) {
      this.parameters[i] = i + 1;
    }
  }

  @Override
  public void setValues(PreparedStatement ps, int i) throws SQLException {
    StatementCreatorUtils.setParameterValue(ps, 1, SqlTypeValue.TYPE_UNKNOWN, Integer.MAX_VALUE);
    StatementCreatorUtils.setParameterValue(ps, 2, SqlTypeValue.TYPE_UNKNOWN, this.parameters[i]);
  }

  @Override
  public int getBatchSize() {
    return this.parameters.length;
  }

  @Override
  public void cleanupParameters() {
    this.parameters = null;
  }

  public boolean isCleanedUp() {
    return this.parameters == null;
  }

}