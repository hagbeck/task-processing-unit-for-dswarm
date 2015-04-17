/*
The MIT License (MIT)

Copyright (c) 2015, Hans-Georg Becker, http://orcid.org/0000-0003-0432-294X

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

package de.tu_dortmund.ub.data.dswarm;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Task Processing Unit for d:swarm
 *
 * @author Dipl.-Math. Hans-Georg Becker (M.L.I.S.)
 * @version 2015-03-19
 *
 */
public class TaskProcessingUnit {

    private static Properties config = new Properties();

    private static Logger logger = Logger.getLogger(TaskProcessingUnit.class.getName());

    public static void main(String[] args) throws Exception {

        // config
        String conffile = "conf" + File.separatorChar + "config.properties";

        // read program parameters
        if (args.length > 0) {

            for (int i = 0; i < args.length; i++) {

                System.out.println("arg = " + args[i]);

                if (args[i].startsWith("-conf=")) {
                    conffile = args[i].split("=")[1];
                }
            }
        }

        // Init properties
        try {
            InputStream inputStream = new FileInputStream(conffile);

            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));

                try {
                    config.load(reader);

                } finally {
                    reader.close();
                }
            }
            finally {
                inputStream.close();
            }
        }
        catch (IOException e) {
            System.out.println("FATAL ERROR: Could not read '" + conffile + "'!");
        }

        // init logger
        PropertyConfigurator.configure(config.getProperty("service.log4j-conf"));

        logger.info("[" + config.getProperty("service.name") + "] " + "Starting 'ExecutionEngine' ...");
        logger.info("[" + config.getProperty("service.name") + "] " + "conf-file = " + conffile);
        logger.info("[" + config.getProperty("service.name") + "] " + "log4j-conf-file = " + config.getProperty("service.log4j-conf"));
        System.out.println("[" + config.getProperty("service.name") + "] " + "Starting 'ExecutionEngine' ...");
        System.out.println("[" + config.getProperty("service.name") + "] " + "conf-file = " + conffile);
        System.out.println("[" + config.getProperty("service.name") + "] " + "log4j-conf-file = " + config.getProperty("service.log4j-conf"));

        String[] files = new File(config.getProperty("resource.watchfolder")).list();
        logger.info("[" + config.getProperty("service.name") + "] " + "Files in " + config.getProperty("resource.watchfolder"));
        logger.info(Arrays.toString(files));
        System.out.println("[" + config.getProperty("service.name") + "] " + "Files in " + config.getProperty("resource.watchfolder"));
        System.out.println(Arrays.toString(files));

        // Init time counter
        long global = System.currentTimeMillis();

        // run ThreadPool
        executeIngests(files);
//        executeTasks(files);

        logger.info("[" + config.getProperty("service.name") + "] " + "d:swarm tasks executed. (Processing time: " + ((System.currentTimeMillis() - global) / 1000) + " s)");
        System.out.println("[" + config.getProperty("service.name") + "] " + "d:swarm tasks executed. (Processing time: " + ((System.currentTimeMillis() - global) / 1000) + " s)");
    }

    private static void executeIngests(String[] files) throws Exception {

        // create job list
        LinkedList<Callable<String>> filesToPush = new LinkedList<Callable<String>>();

        int cnt = 0;
        for (String file : files) {

            cnt++;
            filesToPush.add(new Ingest(config, logger, file, cnt));
        }

        // work on jobs
        ThreadPoolExecutor pool = new ThreadPoolExecutor(Integer.parseInt(config.getProperty("engine.threads")), Integer.parseInt(config.getProperty("engine.threads")), 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

        try {

            List<Future<String>> futureList = pool.invokeAll(filesToPush);

            for (Future<String> f : futureList) {

                String message = f.get();

                logger.info("[" + config.getProperty("service.name") + "] " + message);
                System.out.println("[" + config.getProperty("service.name") + "] " + message);

            }

            pool.shutdown();

        } catch (InterruptedException e) {

            e.printStackTrace();

        } catch (ExecutionException e) {

            e.printStackTrace();
        }
    }

	private static void executeTasks(String[] files) throws Exception {
	
	    // create job list
	    LinkedList<Callable<String>> filesToPush = new LinkedList<Callable<String>>();
	
	    int cnt = 0;
	    for (String file : files) {
	
	        cnt++;
	        filesToPush.add(new Task(config, logger, file, cnt));
	    }
	
	    // work on jobs
	    ThreadPoolExecutor pool = new ThreadPoolExecutor(Integer.parseInt(config.getProperty("engine.threads")), Integer.parseInt(config.getProperty("engine.threads")), 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
	
	    try {
	
	        List<Future<String>> futureList = pool.invokeAll(filesToPush);
	
	        for (Future<String> f : futureList) {
	
	            String message = f.get();
	
	            logger.info("[" + config.getProperty("service.name") + "] " + message);
	            System.out.println("[" + config.getProperty("service.name") + "] " + message);
	
	        }
	
	        pool.shutdown();
	
	    } catch (InterruptedException e) {
	
	        e.printStackTrace();
	
	    } catch (ExecutionException e) {
	
	        e.printStackTrace();
	    }
	}

}
