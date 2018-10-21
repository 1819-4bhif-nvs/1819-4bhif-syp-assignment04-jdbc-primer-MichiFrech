package at.htl.schuelerverwaltung;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SchuelerverwaltungTest {

    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc() {
        //Verbindung zur DB
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich:\n" +
                    e.getMessage() + "\n");
            System.exit(1);
        }

        //Erstellen der DB Tabellen
        try {
            Statement stmt = conn.createStatement();

            String sqlcmd = "CREATE TABLE school_class (" +
                    "   id INT CONSTRAINT class_pk PRIMARY KEY," +
                    "   cl_desc VARCHAR(20) NOT NULL," +
                    "   roomnr VARCHAR(20) NOT NULL)";

            stmt.execute(sqlcmd);

            sqlcmd = "CREATE TABLE student (" +
                    "  id INT CONSTRAINT student_pk PRIMARY KEY" +
                    "  GENERATED ALWAYS AS IDENTITY  (START WITH 1, INCREMENT BY 1)," +
                    "  class_id INT," +
                    "  name varchar(255) NOT NULL," +
                    "  zip_code INT NOT NULL," +
                    "  city varchar(255) NOT NULL," +
                    "  street varchar(255) NOT NULL," +
                    "  year_of_birth INT NOT NULL," +
                    "  CONSTRAINT class_fk FOREIGN KEY (class_id) REFERENCES school_class(id))";

            stmt.execute(sqlcmd);
            conn.commit();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @AfterClass
    public static void teardownJdbc() {
        //Tabellen löschen
        try {
            conn.createStatement().execute("DROP TABLE student");
            System.out.println("Tabelle student gelöscht!");

            conn.createStatement().execute("DROP TABLE school_class");
            System.out.println("Tabelle class gelöscht!");
        } catch (SQLException e) {
            System.out.println("Tabelle class bzw. student konnte nicht gelöscht werden:\n" +
                    e.getMessage());
        }

        //Connection schließen
        try {
            if (conn != null || !conn.isClosed()) {
                conn.close();
                System.out.println("Goodbye!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t01InsertTestClass() {
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();
            String sqlcmd = "INSERT INTO school_class (id, cl_desc, roomnr) values(1, '4BHIF', 'E72')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO school_class (id, cl_desc, roomnr) values(2, '2DHIF', 'E23')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO school_class (id, cl_desc, roomnr) values(3, '3AHITM', '154')";
            countInserts += stmt.executeUpdate(sqlcmd);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts, is(3));
    }

    @Test
    public void t02InsertTestStudents() {
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();
            String sqlcmd = "INSERT INTO student (class_id, name, zip_code, city, street, year_of_birth) values(1, 'Max M1', 4060, 'Leonding', 'TestStr', 2000)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO student (class_id, name, zip_code, city, street, year_of_birth) values(1, 'Max M2', 4060, 'Leonding', 'TestStr', 2000)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO student (class_id, name, zip_code, city, street, year_of_birth) values(1, 'Max M3', 4060, 'Leonding', 'TestStr', 2000)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO student (class_id, name, zip_code, city, street, year_of_birth) values(2, 'Max M4', 4060, 'Leonding', 'TestStr', 2002)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO student (class_id, name, zip_code, city, street, year_of_birth) values(2, 'Max M5', 4060, 'Leonding', 'TestStr', 2002)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO student (class_id, name, zip_code, city, street, year_of_birth) values(3, 'Max M6', 4060, 'Leonding', 'TestStr', 2001)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO student (class_id, name, zip_code, city, street, year_of_birth) values(3, 'Max M7', 4060, 'Leonding', 'TestStr', 2001)";
            countInserts += stmt.executeUpdate(sqlcmd);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts, is(7));
    }

    @Test
    public void t03CheckDataClass() {
        PreparedStatement pstmt = null;

        try {
            pstmt = conn.prepareStatement("SELECT id, cl_desc, roomnr FROM school_class");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("cl_desc"), is("4BHIF"));
            assertThat(rs.getString("roomnr"), is("E72"));
            rs.next();
            assertThat(rs.getString("cl_desc"), is("2DHIF"));
            assertThat(rs.getString("roomnr"), is("E23"));
            rs.next();
            assertThat(rs.getString("cl_desc"), is("3AHITM"));
            assertThat(rs.getString("roomnr"), is("154"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t04CheckDataStudent() {
        PreparedStatement pstmt = null;

        try {
            pstmt = conn.prepareStatement("SELECT id, name, city, year_of_birth FROM student");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"), is("Max M1"));
            assertThat(rs.getString("city"), is("Leonding"));
            assertThat(rs.getString("year_of_birth"), is("2000"));
            rs.next();
            assertThat(rs.getString("name"), is("Max M2"));
            assertThat(rs.getString("city"), is("Leonding"));
            assertThat(rs.getString("year_of_birth"), is("2000"));
            rs.next();
            assertThat(rs.getString("name"), is("Max M3"));
            assertThat(rs.getString("city"), is("Leonding"));
            assertThat(rs.getString("year_of_birth"), is("2000"));
            rs.next();
            assertThat(rs.getString("name"), is("Max M4"));
            assertThat(rs.getString("city"), is("Leonding"));
            assertThat(rs.getString("year_of_birth"), is("2002"));
            rs.next();
            assertThat(rs.getString("name"), is("Max M5"));
            assertThat(rs.getString("city"), is("Leonding"));
            assertThat(rs.getString("year_of_birth"), is("2002"));
            rs.next();
            assertThat(rs.getString("name"), is("Max M6"));
            assertThat(rs.getString("city"), is("Leonding"));
            assertThat(rs.getString("year_of_birth"), is("2001"));
            rs.next();
            assertThat(rs.getString("name"), is("Max M7"));
            assertThat(rs.getString("city"), is("Leonding"));
            assertThat(rs.getString("year_of_birth"), is("2001"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t05CheckKeys() {
        PreparedStatement pstmt = null;

        try {
            pstmt = conn.prepareStatement("SELECT std.name, cl.cl_desc FROM student std " +
                    "JOIN school_class cl ON std.class_id = cl.id " +
                    "WHERE cl.id = 3");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"), is("Max M6"));
            assertThat(rs.getString("cl_desc"), is("3AHITM"));
            rs.next();
            assertThat(rs.getString("name"), is("Max M7"));
            assertThat(rs.getString("cl_desc"), is("3AHITM"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
