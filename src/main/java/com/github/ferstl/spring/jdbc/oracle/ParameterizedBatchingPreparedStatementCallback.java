package com.github.ferstl.spring.jdbc.oracle;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementCallback;

import oracle.jdbc.OraclePreparedStatement;

class ParameterizedBatchingPreparedStatementCallback<T> implements PreparedStatementCallback<int[][]> {

  private final ParameterizedPreparedStatementSetter<T> ppss;
  private final int sendBatchSize;
  private final List<T> batchArgs;


  public ParameterizedBatchingPreparedStatementCallback(
      ParameterizedPreparedStatementSetter<T> ppss,
      int sendBatchSize,
      Collection<T> batchArgs) {

    this.ppss = ppss;
    this.sendBatchSize = sendBatchSize < batchArgs.size() ? sendBatchSize : batchArgs.size();
    this.batchArgs = new ArrayList<>(batchArgs);
  }

  @Override
  public int[][] doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
    OraclePreparedStatement ops = (OraclePreparedStatement) ps;

    List<int[]> rowCounts = new ArrayList<>();
    for (int i = 0; i < this.batchArgs.size(); i += this.sendBatchSize) {
      int remainder = this.batchArgs.size() - i;
      int batchSize = remainder < this.sendBatchSize ? remainder : this.sendBatchSize;

      int[] rowCountsCurrentBatch = new int[batchSize];
      rowCounts.add(rowCountsCurrentBatch);
      ops.setExecuteBatch(batchSize);

      List<T> batch = this.batchArgs.subList(i, i + batchSize);
      for (int j = 0; j < batchSize; j++) {
        this.ppss.setValues(ops, batch.get(j));
        rowCountsCurrentBatch[j] = ops.executeUpdate();
      }
    }

    return rowCounts.toArray(new int[rowCounts.size()][]);
  }

}
