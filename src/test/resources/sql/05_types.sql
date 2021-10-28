
ALTER SESSION SET CONTAINER = ORCLPDB1;
ALTER SESSION SET CURRENT_SCHEMA = spring_jdbc_oracle;

BEGIN
  EXECUTE IMMEDIATE 'CREATE OR REPLACE TYPE test_array_type IS TABLE OF NUMBER';
  -- https://stackoverflow.com/questions/10848277/difference-between-object-and-record-type
  -- EXECUTE IMMEDIATE 'CREATE OR REPLACE TYPE number_varchar_record IS RECORD (value1 NUMBER, value2 VARCHAR2)';
  EXECUTE IMMEDIATE 'CREATE OR REPLACE TYPE number_varchar_record AS OBJECT (value1 NUMBER, value2 VARCHAR2(255))';
  EXECUTE IMMEDIATE 'CREATE OR REPLACE TYPE number_varchar_array_type IS TABLE OF number_varchar_record';
END;
/
