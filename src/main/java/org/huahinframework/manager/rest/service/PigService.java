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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.wink.common.internal.utils.MediaTypeUtils;
import org.apache.wink.common.model.multipart.InMultiPart;
import org.huahinframework.manager.response.Response;
import org.json.JSONObject;

/**
 *
 */
@Path("/pig")
public class PigService extends Service {
    private static final Log log = LogFactory.getLog(PigService.class);

    private static final String JSON_DUMP = "dump";
    private static final String JSON_QUERY = "query";

    private Properties properties = new Properties();

    @Path("/store")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaTypeUtils.MULTIPART_FORM_DATA)
    public JSONObject store(InMultiPart inMP) {
        Map<String, String> status = new HashMap<String, String>();
        status.put(Response.STATUS, "SCCESS");
        try {
            if (!inMP.hasNext()) {
                status.put(Response.STATUS, "ARGUMENTS is empty");
                return new JSONObject(status);
            }

            JSONObject argument = createJSON(inMP.next().getInputStream());
            String query = argument.getString(JSON_QUERY);
            if (query == null || query.isEmpty()) {
                status.put(Response.STATUS, "Query is empty");
                return new JSONObject(status);
            }

            PigServer server = new PigServer(ExecType.MAPREDUCE, properties);
            server.registerQuery(query);
            server.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            status.put(Response.STATUS, e.getMessage());
        }

        return new JSONObject(status);
    }

    @Path("/dump")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaTypeUtils.MULTIPART_FORM_DATA)
    public void dump(@Context HttpServletResponse response,
                     InMultiPart inMP) throws IOException {
        OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
        try {
            if (!inMP.hasNext()) {
                throw new RuntimeException("ARGUMENTS is empty");
            }

            JSONObject argument = createJSON(inMP.next().getInputStream());
            String dump = argument.getString(JSON_DUMP);
            if (dump == null || dump.isEmpty()) {
                error(out, "Dump is empty");
                return;
            }

            String query = argument.getString(JSON_QUERY);
            if (query == null || query.isEmpty()) {
                error(out, "Query is empty");
                return;
            }

            PigServer server = new PigServer(ExecType.MAPREDUCE, properties);
            server.registerQuery(query);
            Iterator<Tuple> ite = server.openIterator(dump);
            Schema schema = server.dumpSchema(dump);
            while (ite.hasNext()) {
                JSONObject jsonObject = new JSONObject();
                Tuple t = ite.next();
                for (int i = 0; i < t.size(); i++) {
                    Object o = t.get(i);
                    jsonObject.put(schema.getField(i).alias, o);
                }
                out.write(jsonObject.toString());
                out.flush();
            }
            server.shutdown();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            error(out, e.getMessage());
        }
    }

    /**
     * @param out
     * @param status
     * @throws IOException
     */
    private void error(OutputStreamWriter out, String status)
            throws IOException {
        Map<String, String> m = new HashMap<String, String>();
        m.put(Response.STATUS, status);
        out.write(new JSONObject(m).toString());
        out.flush();
        out.close();
        return;
    }

    /**
    *
    */
   public void init() {
       PropertiesUtil.loadDefaultProperties(properties);
       properties.putAll(ConfigurationUtil.toProperties(getJobConf()));
   }
}
