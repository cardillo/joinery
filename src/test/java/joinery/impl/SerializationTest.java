//package joinery.impl;
//
//import joinery.DataFrame;
//import junit.framework.TestCase;
//import org.junit.Test;
//
//import javax.xml.crypto.Data;
//import java.sql.Connection;
//import java.sql.Date;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//
//public class SerializationTest extends TestCase {
//
//    // issue #92, Use readsql() to read data from the database, the types obtained are not all strings, can be numbers, dates, etc.
//    public void testReadSql() throws SQLException {
//        Connection c = DriverManager.getConnection("jdbc:derby:memory:testdb;create=true");
//        c.createStatement().executeUpdate("create table data2 (a varchar (20), b double )");
//        c.createStatement().executeUpdate("insert into data2 values ('2022-04-24', 20.02)");
//        c.createStatement().executeUpdate("insert into data2 values ('2022-04-25', 40.44)");
////        List<Object> other = DataFrame.readSql(c, "select * from data").flatten();
//        DataFrame<Object> other = DataFrame.readSql(c, "select * from data2");
//        assertTrue(other.get(0, 0) instanceof Date);
//        assertTrue(other.get(1, 0) instanceof Date);
//        assertTrue(other.get(0, 1) instanceof Double);
//        assertTrue(other.get(1, 1) instanceof Double);
//    }
//
//    // issue #92, Use readsql() to read data from the database, the types obtained are not all strings, can be numbers, dates, etc.
//    public void testReadSql2() throws SQLException {
//        Connection c = DriverManager.getConnection("jdbc:derby:memory:testdb;create=true");
//        c.createStatement().executeUpdate("create table data (a varchar (20), b varchar(10) )");
//        c.createStatement().executeUpdate("insert into data values ('04-24', '20')");
//        c.createStatement().executeUpdate("insert into data values ('04-25', '40')");
//        c.createStatement().executeUpdate("insert into data values ('4-40', '60')");
////        List<Object> other = DataFrame.readSql(c, "select * from data").flatten();
//        DataFrame<Object> other = DataFrame.readSql(c, "select * from data");
//        assertTrue(other.get(0, 0) instanceof Date);
//        assertTrue(other.get(1, 0) instanceof Date);
//        assertTrue(other.get(2, 0) instanceof Date);
//        assertTrue(other.get(0, 1) instanceof Integer);
//        assertTrue(other.get(1, 1) instanceof Integer);
//    }
//}