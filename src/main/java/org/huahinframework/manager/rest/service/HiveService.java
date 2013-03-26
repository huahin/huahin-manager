/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.huahinframework.manager.rest.service;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.wink.common.internal.utils.MediaTypeUtils;
import org.apache.wink.common.model.multipart.InMultiPart;
import org.huahinframework.manager.response.Response;
import org.json.JSONObject;

/**
 *
 */
@Path("/hive")
public class HiveService extends Service {
    private static final Log log = LogFactory.getLog(HiveService.class);

    private static final String V1_DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static final String V2_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String V1_CONNECTION_FORMAT = "jdbc:hive://%s/default";
    private static final String V2_CONNECTION_FORMAT = "jdbc:hive2://%s/default";

    private static final String JSON_QUERY = "query";
    private static final String RESULT = "result";

    private String hiveserver;
    private String driverName;
    private String connectionFormat;

    @Path("/execute")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaTypeUtils.MULTIPART_FORM_DATA)
    public void execute(@Context HttpServletResponse response,
                        InMultiPart inMP) throws IOException {
        OutputStreamWriter out = null;
        Map<String, String> status = new HashMap<String, String>();
        Map<String, Object> result = new HashMap<String, Object>();
        try {
            out = new OutputStreamWriter(response.getOutputStream());
            if (!inMP.hasNext()) {
                status.put(Response.STATUS, "Query is empty");
                out.write(new JSONObject(status).toString());
                out.flush();
                out.close();
                return;
            }

            JSONObject argument = createJSON(inMP.next().getInputStream());
            String query = argument.getString(JSON_QUERY);
            if (query == null || query.isEmpty()) {
                status.put(Response.STATUS, "Query is empty");
                out.write(new JSONObject(status).toString());
                out.flush();
                out.close();
                return;
            }

            Class.forName(driverName);
            Connection con = DriverManager.getConnection(String.format(connectionFormat, hiveserver), "", "");
            Statement stmt = con.createStatement();

            int queryNo = 1;
            String command = "";
            for (String oneCmd : query.split(";")) {
                if (StringUtils.endsWith(oneCmd, "¥¥")) {
                    command += StringUtils.chop(oneCmd) + ";";
                    continue;
                } else {
                    command += oneCmd;
                }

                if (StringUtils.isBlank(command)) {
                  continue;
                }

                boolean b = stmt.execute(command);
                if (b) {
                    result.clear();
                    result.put(JSON_QUERY, queryNo);

                    ResultSet resultSet = stmt.getResultSet();
                    while (resultSet.next()) {
                        JSONObject jsonObject = new JSONObject();
                        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                            jsonObject.put(resultSet.getMetaData().getColumnName(i), resultSet.getString(i));
                        }
                        result.put(RESULT, jsonObject);

                        out.write(new JSONObject(result).toString());
                        out.flush();
                    }

                    if (result.size() == 1) {
                        status.put(Response.STATUS, "SCCESS");
                        result.put(RESULT, status);

                        JSONObject jsonObject = new JSONObject(result);
                        out.write(jsonObject.toString());
                        out.flush();
                    }
                } else {
                    result.clear();
                    status.clear();

                    result.put(JSON_QUERY, queryNo);

                    status.put(Response.STATUS, "SCCESS");
                    result.put(RESULT, status);

                    JSONObject jsonObject = new JSONObject(result);
                    out.write(jsonObject.toString());
                    out.flush();
                }

                command = "";
                queryNo++;
            }

            con.close();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            if (out != null) {
                status.put(Response.STATUS, e.getMessage());
                out.write(new JSONObject(status).toString());
                out.flush();
                out.close();
            }
        }
    }

    @Path("/executeQuery")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaTypeUtils.MULTIPART_FORM_DATA)
    @Deprecated
    public void executeQuery(@Context HttpServletResponse response,
                             InMultiPart inMP) throws IOException {
        OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
        Map<String, String> status = new HashMap<String, String>();
        try {
            if (!inMP.hasNext()) {
                throw new RuntimeException("Query is empty");
            }

            JSONObject argument = createJSON(inMP.next().getInputStream());
            String query = argument.getString(JSON_QUERY);
            if (query == null || query.isEmpty()) {
                status.put(Response.STATUS, "Query is empty");
                out.write(new JSONObject(status).toString());
                out.flush();
                out.close();
                return;
            }

            Class.forName(driverName);
            Connection con = DriverManager.getConnection(String.format(connectionFormat, hiveserver), "", "");
            Statement stmt = con.createStatement();

            ResultSet resultSet = stmt.executeQuery(query);
            while (resultSet.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    jsonObject.put(resultSet.getMetaData().getColumnName(i), resultSet.getString(i));
                }
                out.write(jsonObject.toString());
                out.flush();
            }
            con.close();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            status.put(Response.STATUS, e.getMessage());
            out.write(new JSONObject(status).toString());
            out.flush();
            out.close();
        }
    }

    /**
     *
     */
    public void init() {
        hiveserver = properties.getHiveserver();
        driverName = V1_DRIVER_NAME;
        connectionFormat = V1_CONNECTION_FORMAT;
        if (properties.getHiveserverVersion() == 2) {
            driverName = V2_DRIVER_NAME;
            connectionFormat = V2_CONNECTION_FORMAT;
        }
    }
}
