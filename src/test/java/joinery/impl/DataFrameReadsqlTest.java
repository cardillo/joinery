package joinery.impl;
import joinery.DataFrame;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class DataFrameReadsqlTest {

    // issue #92, Use readsql() to read data from the database, the types obtained are not all strings, can be numbers, dates, etc.
    //CS304 (manually written) https://github.com/cardillo/joinery/issues/92
    @Test
    public void testReadSql() throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:derby:memory:testdb;create=true");
        c.createStatement().executeUpdate("create table data2 (a varchar (20), b double )");
        c.createStatement().executeUpdate("insert into data2 values ('2022-04-24', 20.02)");
        c.createStatement().executeUpdate("insert into data2 values ('2022-04-25', 40.44)");
//        List<Object> other = DataFrame.readSql(c, "select * from data").flatten();
        DataFrame<Object> other = DataFrame.readSql(c, "select * from data2");
        assertTrue(other.get(0, 0) instanceof Date);
        assertTrue(other.get(1, 0) instanceof Date);
        assertTrue(other.get(0, 1) instanceof Double);
        assertTrue(other.get(1, 1) instanceof Double);
    }

    // issue #92, Use readsql() to read data from the database, the types obtained are not all strings, can be numbers, dates, etc.
    //CS304 (manually written) https://github.com/cardillo/joinery/issues/92
    @Test
    public void test2Sql() throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:derby:memory:testdb;create=true");
        c.createStatement().executeUpdate("create table data (a varchar (20), b varchar(10) )");
        c.createStatement().executeUpdate("insert into data values ('04-24', '20')");
        c.createStatement().executeUpdate("insert into data values ('04-25', '40')");
        c.createStatement().executeUpdate("insert into data values ('4-40', '60')");
//        List<Object> other = DataFrame.readSql(c, "select * from data").flatten();
        DataFrame<Object> other = DataFrame.readSql(c, "select * from data");
        assertTrue(other.get(0, 0) instanceof Date);
        assertTrue(other.get(1, 0) instanceof Date);
        assertFalse(other.get(2, 0) instanceof String);
        assertTrue(other.get(0, 1) instanceof Integer);
        assertFalse(other.get(1, 1) instanceof Double);
    }
}
