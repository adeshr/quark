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

package com.qubole.quark.serverclient.server;

import org.apache.calcite.avatica.jdbc.JdbcMeta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by dev on 2/25/16.
 */
public class QuarkMeta extends JdbcMeta {
  protected static final Log LOG = LogFactory.getLog(JdbcMeta.class);

  public QuarkMeta(String url) throws SQLException {
    super(url);
  }

  public QuarkMeta(String url, Properties properties) throws  SQLException {
    super(url, properties);
    LOG.debug("CONSTRUCTOR FOR QUARK META");
  }

  @Override
  public Connection getConnection(String id) throws SQLException {
    LOG.info("CALLING JDBC META GETCONNECTION");
    LOG.info("id is:" + id);
    return super.getConnection(id);
  }

  @Override
  public void openConnection(ConnectionHandle ch, Map<String, String> info) {
    LOG.info("===========================================");
    LOG.info(info);
    LOG.info(ch.id);
    /*try {
      Class.forName("com.qubole.quark.jdbc.QuarkDriver");
      DriverManager.getConnection(
          "jdbc:quark:/home/dev/src/quark/target/test-classes/TpcdsModel.json");
    } catch (Exception e) {
      LOG.error(e.getMessage());
      LOG.error(e.getStackTrace().toString());
    }*/

    try {
      super.openConnection(ch, info);
    } catch (Exception e) {
      LOG.info(e.getMessage(), e);

    }
  }
}
