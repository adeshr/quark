package com.qubole.quark.serverclient.server;

import org.apache.calcite.avatica.Meta;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by dev on 2/24/16.
 */
public class Server {
  public static final String h2Url = "jdbc:h2:mem:TpcdsTest;DB_CLOSE_DELAY=-1";
  public static final String cubeUrl = "jdbc:h2:mem:TpcdsCubes;DB_CLOSE_DELAY=-1";
  public static final String viewUrl = "jdbc:h2:mem:TpcdsViews;DB_CLOSE_DELAY=-1";

  @BeforeClass
  public static void setUp() throws SQLException, IOException, URISyntaxException,
      ClassNotFoundException {
    setupTables(h2Url, "tpcds.sql");
    setupTables(cubeUrl, "tpcds_cubes.sql");
    setupTables(viewUrl, "tpcds_views.sql");
  }

  public static void setupTables(String dbUrl, String filename)
      throws ClassNotFoundException, SQLException, IOException, URISyntaxException {
    Class.forName("org.h2.Driver");
    Properties props = new Properties();
    props.setProperty("user", "sa");
    props.setProperty("password", "");

    Connection connection = DriverManager.getConnection(dbUrl, props);

    Statement stmt = connection.createStatement();
    java.net.URL url = Server.class.getResource("/" + filename);
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    String sql = new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");

    stmt.execute(sql);
  }
  @Test
  public void testStart() {
    Main main = new Main();
    main.run();
    System.out.print("ABCD");
  }

  @Test
  public void testMeta() throws SQLException, ClassNotFoundException {
    Properties props = new Properties();
    String jsonString =
        "   {"
            + "     \"url\":\"jdbc:mysql://localhost.localdomain:3306/nezha_rstore\","
            + "     \"username\":\"root\","
            + "     \"password\":\"\","
            + "     \"encrypt_key\":\"easy\""
            + "   }";
    props.put("dbCredentials", jsonString);
    props.put("schemaFactory", "com.qubole.quark.catalog.db.SchemaFactory");
    Map<String, String> map = new HashMap<>();
    map.put("dbCredentials", props.getProperty("dbCredentials"));
    map.put("schemaFactory", props.getProperty("schemaFactory"));
    QuarkMeta quarkMeta = new QuarkMeta("jdbc:quark:", props);
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle("abcdefghijklmnopqrstuvwxyz");
    Class.forName("com.qubole.quark.jdbc.QuarkDriver");
    quarkMeta.openConnection(ch, map);

    Connection connection = quarkMeta.getConnection("abcdefghijklmnopqrstuvwxyz");
    String query = "select * from canonical.public.web_returns";
    ResultSet resultSet = connection.createStatement().executeQuery(query);
    System.out.println("abcd");

  }
}
