package com.nodusdb.jdbc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Properties;

/**
 * Comprehensive test suite designed to aggressively exercise the PostgreSQL JDBC driver 
 * against NodusDB. The goal is to uncover unimplemented wire protocol messages, 
 * missing pg_catalog tables, and unsupported data types.
 */
public class JdbcFeatureCoverageTest {

    private Connection getConnection() throws Exception {
        String url = System.getenv("NODUS_JDBC_URL");
        if (url == null || url.trim().isEmpty()) {
            throw new IllegalStateException("NODUS_JDBC_URL environment variable is not set");
        }
        
        Properties props = new Properties();
        props.setProperty("user", "nodus");
        props.setProperty("password", "nodus");
        props.setProperty("ApplicationName", "pgjdbc-compat-tests");
        // Bumped to 18.0 as requested. This forces the pgjdbc driver into modern modes,
        // altering how it issues catalog queries and handles metadata.
        props.setProperty("assumeMinServerVersion", "18.0"); 
        
        return DriverManager.getConnection(url, props);
    }

    @Test
    public void testDatabaseMetaData() throws Exception {
        // Many custom databases fail here because pgjdbc relies heavily on pg_catalog 
        // tables (pg_class, pg_attribute, pg_type) to fulfill metadata requests.
        try (Connection conn = getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            
            assertNotNull(meta.getDatabaseProductName());
            assertTrue(meta.getDatabaseMajorVersion() >= 16);
            
            // Test catalog tables mapping
            try (ResultSet rs = meta.getTables(null, "public", "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    assertNotNull(rs.getString("TABLE_NAME"));
                }
            }

            // Test catalog columns mapping
            try (ResultSet rs = meta.getColumns(null, "public", "test_%", "%")) {
                while (rs.next()) {
                    assertNotNull(rs.getString("COLUMN_NAME"));
                    assertTrue(rs.getInt("DATA_TYPE") != 0);
                }
            }
        }
    }

    @Test
    public void testResultSetMetaData() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Requires the database to send RowDescription messages correctly over PG wire
            try (ResultSet rs = stmt.executeQuery("SELECT 1 AS num_col, 'test' AS str_col")) {
                ResultSetMetaData rsMeta = rs.getMetaData();
                
                assertEquals(2, rsMeta.getColumnCount());
                
                assertEquals("num_col", rsMeta.getColumnLabel(1));
                assertEquals(Types.INTEGER, rsMeta.getColumnType(1));
                
                assertEquals("str_col", rsMeta.getColumnLabel(2));
                // Varchar or String representation
                assertTrue(rsMeta.getColumnType(2) == Types.VARCHAR || rsMeta.getColumnType(2) == Types.CHAR);
            }
        }
    }

    @Test
    public void testTransactionsAndAutoCommit() throws Exception {
        try (Connection conn = getConnection()) {
            // This tests whether NodusDB correctly processes BEGIN, COMMIT, ROLLBACK 
            // and SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS test_tx (id INT)");
                stmt.execute("INSERT INTO test_tx (id) VALUES (999)");
            }
            conn.rollback();
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test_tx WHERE id = 999")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Rollback failed to undo the insert");
            }
        }
    }

    @Test
    public void testSavepointsAndGeneratedKeys() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE IF NOT EXISTS test_savepoints (id INT PRIMARY KEY, name TEXT)");

            conn.setAutoCommit(false);
            stmt.execute("INSERT INTO test_savepoints (id, name) VALUES (1, 'kept')");
            Savepoint savepoint = conn.setSavepoint("sp_driver");
            stmt.execute("INSERT INTO test_savepoints (id, name) VALUES (2, 'rolled_back')");
            conn.rollback(savepoint);
            conn.releaseSavepoint(savepoint);
            conn.commit();
            conn.setAutoCommit(true);

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test_savepoints WHERE id = 2")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }

            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO test_savepoints (id, name) VALUES (3, 'generated') RETURNING id")) {
                try (ResultSet keys = pstmt.executeQuery()) {
                    assertTrue(keys.next());
                    assertEquals(3, keys.getInt(1));
                }
            }
        }
    }

    @Test
    public void testStatementConfiguration() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Tests Extended Query Protocol capabilities
            // fetchSize > 0 forces pgjdbc to use portals and Execute messages with row limits
            stmt.setFetchSize(10);
            
            // Tests if the database handles limits applied by the driver
            stmt.setMaxRows(5);
            
            // Tests statement timeouts (often implemented via cancel requests or SET statement_timeout)
            stmt.setQueryTimeout(2); 

            try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testPreparedStatementDataTypes() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute("CREATE TABLE IF NOT EXISTS test_types (" +
                         "id INT PRIMARY KEY, " +
                         "str_val TEXT, " +
                         "bool_val BOOLEAN, " +
                         "double_val DOUBLE PRECISION)");
            
            // PreparedStatement heavily exercises the Parse, Bind, Describe, and Execute PG wire messages
            String sql = "INSERT INTO test_types (id, str_val, bool_val, double_val) VALUES (?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, 100);
                pstmt.setString(2, "TestString");
                pstmt.setBoolean(3, true);
                pstmt.setDouble(4, 3.14159);
                assertEquals(1, pstmt.executeUpdate());
            }
            
            // Null binding
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, 101);
                pstmt.setNull(2, Types.VARCHAR);
                pstmt.setNull(3, Types.BOOLEAN);
                pstmt.setNull(4, Types.DOUBLE);
                assertEquals(1, pstmt.executeUpdate());
            }
            
            // Retrieving Data Types
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM test_types WHERE id = ?")) {
                pstmt.setInt(1, 101);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    
                    rs.getString("str_val");
                    assertTrue(rs.wasNull());
                    
                    rs.getBoolean("bool_val");
                    assertTrue(rs.wasNull());
                }
            }
        }
    }

    @Test
    public void testBatchExecution() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute("CREATE TABLE IF NOT EXISTS test_batch (id INT)");
            
            // Batching relies on the pgjdbc driver packing multiple Bind/Execute messages
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO test_batch (id) VALUES (?)")) {
                pstmt.setInt(1, 1);
                pstmt.addBatch();
                
                pstmt.setInt(1, 2);
                pstmt.addBatch();
                
                pstmt.setInt(1, 3);
                pstmt.addBatch();
                
                int[] results = pstmt.executeBatch();
                System.out.println("Batch results length: " + results.length);
                for (int i = 0; i < results.length; i++) {
                    System.out.println("Result " + i + ": " + results[i]);
                }
                assertEquals(3, results.length);
                assertEquals(1, results[0]);
            }
        }
    }
}
