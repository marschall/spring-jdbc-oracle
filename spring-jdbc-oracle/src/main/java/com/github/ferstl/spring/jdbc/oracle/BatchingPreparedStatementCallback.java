package com.github.ferstl.spring.jdbc.oracle;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.InterruptibleBatchPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementCallback;

import oracle.jdbc.OraclePreparedStatement;

class BatchingPreparedStatementCallback implements PreparedStatementCallback<int[]> {

  private final int sendBatchSize;
  private final BatchPreparedStatementSetter pss;

  BatchingPreparedStatementCallback(int sendBatchSize, BatchPreparedStatementSetter pss) {
    this.sendBatchSize = sendBatchSize;
    this.pss = pss;
  }

  @Override
  public int[] doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
    OraclePreparedStatement ops = (OraclePreparedStatement) ps;
    int batchSize = this.pss.getBatchSize();

    // Don't use an int[] array here because instances of InterruptibleBatchPreparedStatementSetter
    // might return Integer.MAX_VALUE as batch size.
    List<Integer> rowCounts = new ArrayList<>();

    if (this.pss instanceof InterruptibleBatchPreparedStatementSetter) {
      InterruptibleBatchPreparedStatementSetter ipss = (InterruptibleBatchPreparedStatementSetter) this.pss;
      executeUpdate(ops, ipss, rowCounts);
    } else {
      int completeBatchSize = (batchSize / this.sendBatchSize) * this.sendBatchSize;
      int remainingBatchSize = batchSize % this.sendBatchSize;
      executeUpdate(ops, rowCounts, 0, completeBatchSize);
      executeUpdate(ops, rowCounts, completeBatchSize, completeBatchSize + remainingBatchSize);
    }

    return toIntArray(rowCounts);
  }

  private void executeUpdate(OraclePreparedStatement ops, List<Integer> rowCounts, int start, int end)
  throws SQLException {

    int batchSize = end - start;

    if (batchSize > 0) {
      int sendBatchSize = this.sendBatchSize < batchSize ? this.sendBatchSize : batchSize;
      ops.setExecuteBatch(sendBatchSize);

      for (int i = start; i < end; i++) {
        this.pss.setValues(ops, i);
        rowCounts.add(ops.executeUpdate());
      }
    }
  }

  private void executeUpdate(OraclePreparedStatement ops, InterruptibleBatchPreparedStatementSetter ipss, List<Integer> rowCounts)
  throws SQLException {

    ops.setExecuteBatch(this.sendBatchSize);
    int i = 0;
    while (i < ipss.getBatchSize()) {
      if (ipss.isBatchExhausted(i)) {
        break;
      }
      ipss.setValues(ops, i);
      rowCounts.add(ops.executeUpdate());
      i++;
    }

    if (i > 0 && i % this.sendBatchSize != 0) {
      rowCounts.set(rowCounts.size() - 1, ops.sendBatch());
    }

  }

  private static int[] toIntArray(List<Integer> intList) {
    int[] array = new int[intList.size()];
    int i = 0;
    for (Integer integer : intList) {
      array[i++] = integer;
    }
    return array;
  }
}