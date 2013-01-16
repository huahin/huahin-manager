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
package org.huahinframework.manager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.huahinframework.manager.queue.QueueManager;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.webapp.WebAppContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 *
 */
public class Runner {
    private static final Log log = LogFactory.getLog(Runner.class);

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("argument error: [war path] [port]");
            System.exit(-1);
        }

        String war = args[0];
        int port = Integer.valueOf(args[1]);

        Runner runner = new Runner();
        runner.start(war, port);

        System.exit(0);
    }

    /**
     * @param war
     * @param port
     */
    public void start(String war, int port) {
        log.info("huahin-manager start");

        ConfigurableApplicationContext applicationContext = null;
        try {
            applicationContext
                = new ClassPathXmlApplicationContext("huahinManagerProperties.xml");
            Properties properties = (Properties) applicationContext.getBean("properties");

            Thread queueManager = new QueueManager(properties);
            queueManager.start();

            SelectChannelConnector connector = new SelectChannelConnector();
            connector.setPort(port);

            Server server = new Server();
            server.setConnectors(new Connector[] { connector });

            WebAppContext web = new WebAppContext();
            web.setContextPath("/");
            web.setWar(war);

            server.addHandler(web);
            server.start();
            server.join();
            queueManager.join();
        } catch (Exception e) {
            log.error("huahin-manager aborted", e);
            System.exit(-1);
        } finally {
            if (applicationContext != null) {
                applicationContext.close();
            }
        }

        log.info("huahin-manager end");
    }
}
