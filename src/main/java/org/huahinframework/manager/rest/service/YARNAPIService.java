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

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.wink.client.Resource;
import org.apache.wink.client.RestClient;
import org.huahinframework.manager.response.Response;
import org.json.JSONObject;

/**
 *
 */
@Path("/api")
public class YARNAPIService extends Service {
    private static final Log log = LogFactory.getLog(YARNAPIService.class);

    private static final String HTTP = "http://";

    @Path("/rm/{tmp : .+}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject getRM(@Context HttpServletRequest request) {
        String path = request.getRequestURI().replace("/api/rm", "");
        return get(HTTP + properties.getYarnResourcemanagerWebappAddress() + path);
    }

    @Path("/nm/{tmp : .+}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject getNM(@Context HttpServletRequest request) {
        String path = request.getRequestURI().replace("/api/nm", "");
        return get(HTTP + properties.getYarnNodemanagerWebappAddress() + path);
    }

    @Path("/proxy/{tmp : .+}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject getProxy(@Context HttpServletRequest request) {
        String path = request.getRequestURI().replace("/api", "");
        return get(HTTP + properties.getYarnWebProxyAddress() + path);
    }

    @Path("/history/{tmp : .+}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject getHistory(@Context HttpServletRequest request) {
        String path = request.getRequestURI().replace("/api/history", "");
        return get(HTTP + properties.getMapreduceJobhistoryWebappAddress() + path);
    }

    /**
     * @param uri
     * @return JSONObject
     */
    private JSONObject get(String uri) {
        JSONObject jsonObject = null;
        try {
            RestClient client = new RestClient();
            Resource resource = client.resource(uri);
            jsonObject = resource.get(JSONObject.class);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            Map<String, String> status = new HashMap<String, String>();
            status.put(Response.STATUS, e.getMessage());
            jsonObject = new JSONObject(status);
        }

        return jsonObject;
    }
}
