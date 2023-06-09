-- https://ignite.apache.org/docs/latest/sql-reference/data-types
create table rainbow (
     id UUID, -- java.util.UUID
     big BIGINT, -- java.lang.Long
     bool BOOLEAN, -- java.lang.Boolean
     dec DECIMAL, -- java.math.BigDecimal
     double DOUBLE, -- java.lang.Double
     int INT, -- java.lang.Integer
     real REAL, -- java.lang.Float
     small SMALLINT, -- java.lang.Short
     tiny TINYINT, -- java.lang.Byte
     char CHAR, -- java.lang.String
     var VARCHAR, -- java.lang.String
     date DATE, -- java.sql.Date
     time TIME, -- java.sql.Time
     ts TIMESTAMP, -- java.sql.Timestamp
     bin BINARY, -- byte[]
     primary key (id)
);
