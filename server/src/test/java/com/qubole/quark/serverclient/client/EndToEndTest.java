/*
 * Copyright (c) 2015. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.qubole.quark.serverclient.client;

import com.qubole.quark.serverclient.server.Main;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Created by adeshr on 2/24/16.
 */
public class EndToEndTest {

  public static final String h2Url = "jdbc:h2:mem:TpcdsTest;DB_CLOSE_DELAY=-1";
  public static final String cubeUrl = "jdbc:h2:mem:TpcdsCubes;DB_CLOSE_DELAY=-1";
  public static final String viewUrl = "jdbc:h2:mem:TpcdsViews;DB_CLOSE_DELAY=-1";

  @BeforeClass
  public static void setUp() throws SQLException, IOException, URISyntaxException,
      ClassNotFoundException {
    String[] args = new String [1];
    args[0] = "jsonCatalog.json";
    new Thread(new Main(args)).start();

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
    java.net.URL url = EndToEndTest.class.getResource("/" + filename);
    java.nio.file.Path resPath = java.nio.file.Paths.get(url.toURI());
    String sql = new String(java.nio.file.Files.readAllBytes(resPath), "UTF8");

    stmt.execute(sql);
  }

  @Test
  public void testClient() throws SQLException, ClassNotFoundException {
    Class.forName("com.qubole.quark.jdbc.QuarkDriver");
    QuarkDriver d = new QuarkDriver();
    Class.forName("org.h2.Driver");
    Connection connection = d.connect(ThinClientUtil.getConnectionUrl("0.0.0.0", 8765), new Properties());

    String query = "select * from canonical.public.web_returns";
    ResultSet resultSet = connection.createStatement().executeQuery(query);

    List<String> wrItemSk = new ArrayList<String>();
    List<String> wrOrderNumber = new ArrayList<String>();
    while (resultSet.next()) {
      wrItemSk.add(resultSet.getString("wr_item_sk"));
      wrOrderNumber.add(resultSet.getString("wr_order_number"));
    }

    assertThat(wrItemSk.size(), equalTo(1));
    assertThat(wrOrderNumber.size(), equalTo(1));
    assertThat(wrItemSk.get(0), equalTo("1"));
    assertThat(wrOrderNumber.get(0), equalTo("10"));
  }
}
