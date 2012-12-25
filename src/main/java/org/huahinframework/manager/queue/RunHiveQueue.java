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
package org.huahinframework.manager.queue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.huahinframework.manager.Properties;

/**
 *
 */
public class RunHiveQueue extends Thread {
    private static final Log log = LogFactory.getLog(RunHiveQueue.class);

    private static final String V1_DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static final String V2_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String V1_CONNECTION_FORMAT = "jdbc:hive://%s/default";
    private static final String V2_CONNECTION_FORMAT = "jdbc:hive2://%s/default";

    private String hiveserver;
    private String driverName;
    private String connectionFormat;
    private String queuePath;
    private Queue queue;

    /**
     * @param properties
     * @param queuePath
     * @param queue
     */
    public RunHiveQueue(Properties properties, String queuePath, Queue queue) {
        this.queuePath = queuePath;
        this.queue = queue;
        this.hiveserver = properties.getHiveserver();
        this.driverName = V1_DRIVER_NAME;
        this.connectionFormat = V1_CONNECTION_FORMAT;
        if (properties.getHiveserverVersion() == 2) {
            this.driverName = V2_DRIVER_NAME;
            this.connectionFormat = V2_CONNECTION_FORMAT;
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        try {
            Class.forName(driverName);
            Connection con = DriverManager.getConnection(String.format(connectionFormat, hiveserver), "", "");
            Statement stmt = con.createStatement();

            stmt.executeQuery(queue.getScript());

            con.close();
        } catch (Exception e) {
            queue.setMessage(e.toString());
            try {
                QueueUtils.registerQueue(queuePath, queue);
            } catch (IOException e1) {
                e1.printStackTrace();
                log.error(e1);
            }
            e.printStackTrace();
            log.error(e);
        }
    }
}
