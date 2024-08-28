/**
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2017
 * <p>
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 */
package com.ericsson.component.aia.services.bps.test.cluster.util;

import static org.junit.Assert.fail;

import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.test.enums.ScenarioType;
import com.sshtools.scp.ScpClient;
import com.sshtools.ssh.SshClient;
import com.sshtools.ssh.SshSession;

/**
 * To log in to cluster using public key Authentication.
 *
 * Note : we are currently using SSH authentication to transfer files on remote cluster to HDFS and vice versa because at the moment we have problem
 * using HDFS API to copy files due to Cluster Internal - External ip Configuration, Once It will be sorted we will refactor this class and will
 * remove this ssh authentication implmenetation.
 */
public class SshTerminalClientUtil extends SshClientConnection {

    /**
     * Used to copy files on the remote server.
     * 
     * @param itemToCopy
     *            File to copy
     * @param dir
     *            Directory on the remote server
     */
    public String copyFiles(String itemToCopy, String dir, final ScenarioType runningTestType) {
        try {

            if (StringUtils.isBlank(dir)) {
                dir = getTestDir();
            } else {
                setTestDir(dir);
            }

            final SshClient ssh = getSshClient(runningTestType);

            if (ssh.isAuthenticated()) {
                SshSession session = ssh.openSessionChannel();
                session.requestPseudoTerminal("vt100", 80, 24, 0, 0);
                session.startShell();
                session.getClient().openSessionChannel().executeCommand(MAKEDIR + dir);
                ScpClient scpClient = new ScpClient(ssh);
                scpClient.put(itemToCopy, dir, true);
                itemToCopy = itemToCopy.substring(itemToCopy.lastIndexOf("/") + 1);
                filePath = dir + Constants.SEPARATOR + itemToCopy;
                System.out.print("Files Copied on the Server: \n");
            }
        } catch (Throwable th) {
            th.printStackTrace();
            fail(th.getMessage());
        }

        return filePath;
    }

    /**
     * To Copy Files form cluster to HDFS.
     * 
     * @return File Path from HDFS where files are copied.
     */

    public String copyFilesOnCluster(final String filePath, final ScenarioType runnigTestType) {
        try {
            final SshClient ssh = getSshClient(runnigTestType);
            if (ssh.isAuthenticated()) {
                SshSession session = ssh.openSessionChannel();
                session.requestPseudoTerminal("vt100", 80, 24, 0, 0);
                session.startShell();
                final String[] commands = getCommandsForCopyFilesFromClusterToFs(filePath, runnigTestType);
                for (int i = 0; i < commands.length; i++) {
                    session.getClient().openSessionChannel().executeCommand(commands[i]);
                    Thread.sleep(5000);
                }
            }

        } catch (Throwable th) {
            th.printStackTrace();
            fail(th.getMessage());
        }
        return this.filePath;
    }

    private String[] getCommandsForCopyFilesFromClusterToFs(String filePath, final ScenarioType runnigTestType) {

        final String dir = getDirectoryForTextFile(filePath);

        switch (runnigTestType) {

            case ALLUXIO:
                return new String[] { ALLUXIO_FS + MAKEDIR + getTestDir() + Constants.SEPARATOR,
                        ALLUXIO_FS + CHANGE_PERMISSION + getTestDir() + Constants.SEPARATOR,
                        ALLUXIO_FS + COPYFROMLOCAL + filePath + " " + getTestDir() + Constants.SEPARATOR };

            case HDFS:
                return new String[] { HADOOP_FS + "-" + MAKEDIR + " -p " + dir + Constants.SEPARATOR,
                        HADOOP_FS + " -" + CHANGE_PERMISSION + dir + Constants.SEPARATOR,
                        HADOOP_FS + PUT_FILE + filePath + " " + dir + Constants.SEPARATOR };

            default:
                return null;

        }

    }

    private String getDirectoryForTextFile(String itemToCopy) {
        itemToCopy = itemToCopy.substring(itemToCopy.lastIndexOf("/") + 1);
        if (itemToCopy.equalsIgnoreCase("SalesJan2009.textfile")) {
            return getTestDir() + "/file";
        } else
            return getTestDir();
    }

    /**
     * To Coy files from HDFS to Cluster
     * 
     * @param path
     *            path of the file from HDFS
     *
     * @param dir
     *            Directory on cluster where files needs to be copied from HDFS
     */

    public void copyFilesFromFSToCluster(final Path path, String dir, final ScenarioType runningTestType) {
        try {
            final SshClient ssh = getSshClient(runningTestType);
            if (ssh.isAuthenticated()) {
                SshSession session = ssh.openSessionChannel();
                session.requestPseudoTerminal("vt100", 80, 24, 0, 0);
                session.startShell();
                REMOTE_DIR = dir + "output" + Constants.SEPARATOR;
                final String[] commands = getCommandsForCopyFilesFromFsToCluster(path, runningTestType);
                for (int i = 0; i < commands.length; i++) {
                    session.getClient().openSessionChannel().executeCommand(commands[i]);
                }
            }

        } catch (Throwable th) {
            th.printStackTrace();
            fail(th.getMessage());
        }
    }

    private String[] getCommandsForCopyFilesFromFsToCluster(final Path path, final ScenarioType runningTestType) {

        switch (runningTestType) {
            case HDFS:
                return new String[] { MAKEDIR + "-p " + REMOTE_DIR, HADOOP_FS + GET_FILE + path + " " + REMOTE_DIR };
            case ALLUXIO:
                return new String[] { MAKEDIR + " " + REMOTE_DIR, ALLUXIO_FS + COPYTOLOCAL + path + " " + REMOTE_DIR };
            default:
                return null;
        }
    }

    /**
     * To Copy Files from remote server to Local file System
     * 
     * @param file
     *            Local directory where files to be copied.
     * @return Local directory location where files are copied from remote server.
     */
    public File copyFilesFromClusterToLocal(final String file, final ScenarioType runningTestType) {
        File localDir = null;
        try {
            final SshClient ssh = getSshClient(runningTestType);
            if (ssh.isAuthenticated()) {
                SshSession session = ssh.openSessionChannel();
                session.requestPseudoTerminal("vt100", 80, 24, 0, 0);
                session.startShell();
                ScpClient scpClient = new ScpClient(ssh);
                scpClient.get(file, REMOTE_DIR, true);
                localDir = new File(REMOTE_DIR);
            }

        } catch (Throwable th) {
            th.printStackTrace();
            fail(th.getMessage());
        }
        return localDir;

    }

    public void cleanUpFromCluster(final String testDirectory, final ScenarioType scenarioType) {
        try {
            final SshClient ssh = getSshClient(scenarioType);
            if (ssh.isAuthenticated()) {
                SshSession session = ssh.openSessionChannel();
                session.requestPseudoTerminal("vt100", 80, 24, 0, 0);
                session.startShell();
                final String[] commandToRun = getCommandsToRemoveTestDir(scenarioType);
                for (int i = 0; i < commandToRun.length; i++) {
                    session.getClient().openSessionChannel().executeCommand(commandToRun[i] + testDirectory + "/");
                }
            }
        } catch (Throwable th) {
            th.printStackTrace();
            fail(th.getMessage());
        }
    }

    private String[] getCommandsToRemoveTestDir(final ScenarioType scenarioType) {
        switch (scenarioType) {
            case ALLUXIO:
                return new String[] { ALLUXIO_FS + REMOVE_TEST_FOLDER_FROM_CLUSTER };
            case HDFS:
            case HIVE:
            case JDBC:
            case FILE:
                return new String[] { REMOVE_TEST_FOLDER_FROM_HDFS, REMOVE_TEST_FOLDER_FROM_CLUSTER };
            default:
                return null;
        }
    }

}