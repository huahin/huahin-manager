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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public class QueueUtils {
    private static final SimpleDateFormat KEY_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private static final long REMOVE_PERIOS = 2 * 24 * 60 * 60 * 1000; // 2 days

    /**
     * @param properties
     * @return queue path
     */
    public static String getQueuePath(String path) {
        return path + "/queue/queue/";
    }

    /**
     * @param path
     * @return queue map
     * @throws Exception
     */
    public static synchronized Map<String, Queue> readQueue(String path) throws Exception {
        Map<String, Queue> queueMap = new TreeMap<String, Queue>();

        File filePath = new File(path);
        for (String fileName : filePath.list()) {
            FileInputStream inFile = new FileInputStream(path + fileName);
            ObjectInputStream inObject = new ObjectInputStream(inFile);
            Queue queue = (Queue) inObject.readObject();
            if (!queue.isRun()) {
                synchronized (KEY_FORMAT) {
                    queueMap.put(KEY_FORMAT.format(queue.getDate()), queue);
                }
            }

            inObject.close();
            inFile.close();
        }

        return queueMap;
    }

    /**
     * @param path
     * @return runs queue map
     * @throws Exception
     */
    public static synchronized Map<String, Queue> readRunQueue(String path) throws Exception {
        Map<String, Queue> queueMap = new TreeMap<String, Queue>();

        File filePath = new File(path);
        for (String fileName : filePath.list()) {
            FileInputStream inFile = new FileInputStream(path + fileName);
            ObjectInputStream inObject = new ObjectInputStream(inFile);
            Queue queue = (Queue) inObject.readObject();
            if (queue.isRun()) {
                synchronized (KEY_FORMAT) {
                    queueMap.put(KEY_FORMAT.format(queue.getDate()), queue);
                }
            }

            inObject.close();
            inFile.close();
        }

        return queueMap;
    }

    /**
     * @param path
     * @return runs queue map
     * @throws Exception
     */
    public static synchronized Map<String, Queue> readRemoveQueue(String path) throws Exception {
        Map<String, Queue> queueMap = new TreeMap<String, Queue>();

        File filePath = new File(path);
        for (String fileName : filePath.list()) {
            FileInputStream inFile = new FileInputStream(path + fileName);
            ObjectInputStream inObject = new ObjectInputStream(inFile);
            Queue queue = (Queue) inObject.readObject();
            if (queue.isRun()) {
                long diff = System.currentTimeMillis() - queue.getDate().getTime();
                if (diff < REMOVE_PERIOS) {
                    continue;
                }

                synchronized (KEY_FORMAT) {
                    queueMap.put(KEY_FORMAT.format(queue.getDate()), queue);
                }
            }

            inObject.close();
            inFile.close();
        }

        return queueMap;
    }

    /**
     * @param path
     * @param queue
     * @throws IOException
     */
    public static synchronized void registerQueue(String path, Queue queue) throws IOException {
        synchronized (KEY_FORMAT) {
            String date = KEY_FORMAT.format(queue.getDate());
            queue.setId("Q_" + date);

            FileOutputStream outFile = new FileOutputStream(path + date);
            ObjectOutputStream outObject = new ObjectOutputStream(outFile);
            outObject.writeObject(queue);

            outObject.close();
            outFile.close();
        }
    }

    /**
     * @param path
     * @param queue
     */
    public static void removeQueue(String path, Queue queue) {
        String queueFile = path + queue.getId().replace("Q_", "");
        File tmp = null;
        tmp = new File(queueFile);
        if (tmp.exists()) {
            tmp.delete();
        }
        if (queue.getJar() != null) {
            tmp = new File(queue.getJar());
            if (tmp.exists()) {
                tmp.delete();
            }
        }
    }
}
