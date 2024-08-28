/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2017
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.component.aia.services.bps.test.cluster.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.ericsson.component.aia.services.bps.test.enums.ScenarioType;
import com.sshtools.net.SocketTransport;
import com.sshtools.publickey.InvalidPassphraseException;
import com.sshtools.publickey.SshPrivateKeyFile;
import com.sshtools.publickey.SshPrivateKeyFileFactory;
import com.sshtools.ssh.HostKeyVerification;
import com.sshtools.ssh.PublicKeyAuthentication;
import com.sshtools.ssh.SshAuthentication;
import com.sshtools.ssh.SshClient;
import com.sshtools.ssh.SshConnector;
import com.sshtools.ssh.SshException;
import com.sshtools.ssh.components.SshKeyPair;
import com.sshtools.ssh.components.SshPublicKey;

public abstract class SshClientConnection {

    private Properties properties;

    private String hostName;

    private String userName;

    private String key;

    protected String filePath;

    protected String testDir;

    protected String MAKEDIR = "mkdir ";

    protected String HADOOP_FS = "hadoop fs ";

    protected String ALLUXIO_FS = "alluxio1.3/bin/alluxio fs ";

    protected String CHANGE_PERMISSION = "chmod -R 777 ";

    protected String PUT_FILE = " -put -f ";

    protected String COPYFROMLOCAL = " copyFromLocal ";

    protected String COPYTOLOCAL = " copyToLocal ";

    protected String GET_FILE = " -get -p ";

    protected String REMOVE_TEST_FOLDER_FROM_HDFS = "sudo -u hdfs hadoop fs -rm -R ";

    protected String REMOVE_TEST_FOLDER_FROM_CLUSTER = "rm -R ";

    protected String REMOTE_DIR = "";

    /**
     * Connect to Remote server using key and use the public key to authenticate.
     * 
     * @return ssh Client
     * @throws SshException
     * @throws IOException
     * @throws InvalidPassphraseException
     */

    public SshClient getSshClient(final ScenarioType testType) throws SshException, IOException, InvalidPassphraseException {

        getClusterConfigValue(testType);

        int idx = hostName.indexOf(':');
        int port = 22;
        if (idx > -1) {
            port = Integer.parseInt(hostName.substring(idx + 1));
            hostName = hostName.substring(0, idx);
        }

        System.out.print("Username [Enter for " + System.getProperty("user.name") + "]: ");

        if (userName == null || userName.trim().equals(""))
            userName = System.getProperty("user.name");

        System.out.println("Connecting to " + hostName);

        /**
         * Create an SshConnector instance
         */
        SshConnector con = SshConnector.createInstance();

        // con.setSupportedVersions(1);
        // Lets do some host key verification
        HostKeyVerification hkv = new HostKeyVerification() {
            public boolean verifyHost(String hostname, SshPublicKey key) {
                try {
                    System.out.println("The connected host's key (" + key.getAlgorithm() + ") is");
                    System.out.println(key.getFingerprint());
                } catch (SshException e) {
                }
                return true;
            }
        };

        con.getContext().setHostKeyVerification(hkv);

        /**
         * Connect to the host
         */
        SshClient ssh = con.connect(new SocketTransport(hostName, port), userName, true);

        /**
         * Authenticate the user using password authentication
         */
        PublicKeyAuthentication pk = new PublicKeyAuthentication();

        do {
            System.out.print("Private key file: ");
            SshPrivateKeyFile pkfile = SshPrivateKeyFileFactory.parse(new FileInputStream(System.getProperty("user.home") + key));

            SshKeyPair pair;
            if (pkfile.isPassphraseProtected()) {
                pair = pkfile.toKeyPair(null);
            } else
                pair = pkfile.toKeyPair(null);

            pk.setPrivateKey(pair.getPrivateKey());
            pk.setPublicKey(pair.getPublicKey());
            System.out.print(pair.getPrivateKey() + "\n");
        } while (ssh.authenticate(pk) != SshAuthentication.COMPLETE && ssh.isConnected());
        return ssh;
    }

    private void getClusterConfigValue(final ScenarioType testType) {

        if (testType == ScenarioType.ALLUXIO) {
            hostName = properties.getProperty("alluxioHostName");
            userName = properties.getProperty("alluxioUserName");
            key = properties.getProperty("alluxioKey");
        } else {
            hostName = properties.getProperty("hdfsHostName");
            userName = properties.getProperty("hdfsUserName");
            key = properties.getProperty("hdfsKey");
        }

    }

    public String getTestDir() {
        return testDir;
    }

    public void setTestDir(final String testDir) {
        this.testDir = testDir;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }
}
