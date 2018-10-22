package at.htl.bauernhof;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BauernhofTest {

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

            String sqlcmd = "CREATE TABLE farm (" +
                    "   id INT CONSTRAINT farm_pk PRIMARY KEY," +
                    "   f_name VARCHAR(20) NOT NULL," +
                    "   city VARCHAR(20) NOT NULL)";

            stmt.execute(sqlcmd);

            sqlcmd = "CREATE TABLE animal (" +
                     "   id INT CONSTRAINT animal_pk PRIMARY KEY," +
                     "   animal_type VARCHAR(255) UNIQUE NOT NULL)";

            stmt.execute(sqlcmd);

            sqlcmd = "CREATE TABLE barn (" +
                    "  id INT CONSTRAINT barn_pk PRIMARY KEY," +
                    "  farm_id INT," +
                    "  animal_id INT," +
                    "  CONSTRAINT farm_fk FOREIGN KEY (farm_id) REFERENCES farm(id)," +
                    "  CONSTRAINT animals_fk FOREIGN KEY (animal_id) REFERENCES animal(id))";

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
            conn.createStatement().execute("DROP TABLE barn");
            System.out.println("Tabelle barn gelöscht!");

            conn.createStatement().execute("DROP TABLE farm");
            System.out.println("Tabelle farm gelöscht!");

            conn.createStatement().execute("DROP TABLE animal");
            System.out.println("Tabelle animal gelöscht!");
        } catch (SQLException e) {
            System.out.println("Tabelle farm, animal bzw. barn konnte nicht gelöscht werden:\n" +
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
    public void t01InsertTestFarm() {
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();
            String sqlcmd = "INSERT INTO farm (id, f_name, city) values(1, 'Huber Hof', 'Leonding')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO farm (id, f_name, city) values(2, 'Schmidt Hof', 'Linz')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO farm (id, f_name, city) values(3, 'Mair Hof', 'Wels')";
            countInserts += stmt.executeUpdate(sqlcmd);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts, is(3));
    }

    @Test
    public void t02InsertTestAnimals() {
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();
            String sqlcmd = "INSERT INTO animal (id, animal_type) values(1, 'Kuh')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO animal (id, animal_type) values(2, 'Schaf')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO animal (id, animal_type) values(3, 'Huhn')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO animal (id, animal_type) values(4, 'Stier')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO animal (id, animal_type) values(5, 'Ziege')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO animal (id, animal_type) values(6, 'Kalb')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO animal (id, animal_type) values(7, 'Schwein')";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO animal (id, animal_type) values(8, 'Hase')";
            countInserts += stmt.executeUpdate(sqlcmd);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts, is(8));
    }

    @Test
    public void t03InsertTestBarns() {
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();
            String sqlcmd = "INSERT INTO barn (id, farm_id, animal_id) values(1, 1, 2)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO barn (id, farm_id, animal_id) values(2, 1, 7)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO barn (id, farm_id, animal_id) values(3, 2, 5)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO barn (id, farm_id, animal_id) values(4, 2, 3)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO barn (id, farm_id, animal_id) values(5, 3, 6)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO barn (id, farm_id, animal_id) values(6, 3, 1)";
            countInserts += stmt.executeUpdate(sqlcmd);

            sqlcmd = "INSERT INTO barn (id, farm_id, animal_id) values(7, 3, 4)";
            countInserts += stmt.executeUpdate(sqlcmd);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts, is(7));
    }

    @Test
    public void t04CheckDataClass() {
        PreparedStatement pstmt = null;

        try {
            pstmt = conn.prepareStatement("SELECT id, f_name, city FROM farm");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("f_name"), is("Huber Hof"));
            assertThat(rs.getString("city"), is("Leonding"));
            rs.next();
            assertThat(rs.getString("f_name"), is("Schmidt Hof"));
            assertThat(rs.getString("city"), is("Linz"));
            rs.next();
            assertThat(rs.getString("f_name"), is("Mair Hof"));
            assertThat(rs.getString("city"), is("Wels"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t05CheckDataAnimal() {
        PreparedStatement pstmt = null;

        try {
            pstmt = conn.prepareStatement("SELECT id, animal_type FROM animal");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("animal_type"), is("Kuh"));
            rs.next();
            assertThat(rs.getString("animal_type"), is("Schaf"));
            rs.next();
            assertThat(rs.getString("animal_type"), is("Huhn"));
            rs.next();
            assertThat(rs.getString("animal_type"), is("Stier"));
            rs.next();
            assertThat(rs.getString("animal_type"), is("Ziege"));
            rs.next();
            assertThat(rs.getString("animal_type"), is("Kalb"));
            rs.next();
            assertThat(rs.getString("animal_type"), is("Schwein"));
            rs.next();
            assertThat(rs.getString("animal_type"), is("Hase"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t06CheckDataBarn() {
        PreparedStatement pstmt = null;

        try {
            pstmt = conn.prepareStatement("SELECT farm_id, animal_id FROM barn");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getInt("farm_id"), is(1));
            assertThat(rs.getInt("animal_id"), is(2));
            rs.next();
            assertThat(rs.getInt("farm_id"), is(1));
            assertThat(rs.getInt("animal_id"), is(7));
            rs.next();
            assertThat(rs.getInt("farm_id"), is(2));
            assertThat(rs.getInt("animal_id"), is(5));
            rs.next();
            assertThat(rs.getInt("farm_id"), is(2));
            assertThat(rs.getInt("animal_id"), is(3));
            rs.next();
            assertThat(rs.getInt("farm_id"), is(3));
            assertThat(rs.getInt("animal_id"), is(6));
            rs.next();
            assertThat(rs.getInt("farm_id"), is(3));
            assertThat(rs.getInt("animal_id"), is(1));
            rs.next();
            assertThat(rs.getInt("farm_id"), is(3));
            assertThat(rs.getInt("animal_id"), is(4));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t07CheckKeys() {
        PreparedStatement pstmt = null;

        try {
            pstmt = conn.prepareStatement("SELECT farm.f_name, animal.animal_type FROM farm " +
                    "JOIN barn ON barn.farm_id = farm.id " +
                    "JOIN animal ON animal.id = barn.animal_id " +
                    "WHERE farm.id = 1");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("f_name"), is("Huber Hof"));
            assertThat(rs.getString("animal_type"), is("Schaf"));
            rs.next();
            assertThat(rs.getString("f_name"), is("Huber Hof"));
            assertThat(rs.getString("animal_type"), is("Schwein"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
