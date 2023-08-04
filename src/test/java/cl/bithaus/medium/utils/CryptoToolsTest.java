package cl.bithaus.medium.utils;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoToolsTest {

    public static final Logger logger =  LoggerFactory.getLogger(CryptoToolsTest.class);

    @Test
    public void testLoadX509Certificate() throws Exception {

        String caCert = "-----BEGIN CERTIFICATE-----\n" +

			"-----END CERTIFICATE-----";

        // write the certificate to a file
        String filename = "/tmp/certfile";

        try {

            Files.write(java.nio.file.Paths.get(filename), caCert.getBytes(), StandardOpenOption.CREATE);
            
            X509Certificate result = CryptoTools.loadX509Certificate(filename);
            
            logger.info(result.toString());

            assertNotNull(result);
        }
        finally {

            Files.delete(java.nio.file.Paths.get(filename));
        }
    }

    @Test
    public void testLoadRSAPrivateKey() throws Exception {

        String keyStr = "-----BEGIN RSA PRIVATE KEY-----\n" +

        "-----END RSA PRIVATE KEY-----";
        
        String filename = "/tmp/keyfile";

        try {

            Files.write(java.nio.file.Paths.get(filename), keyStr.getBytes(), StandardOpenOption.CREATE);
            
            PrivateKey result = CryptoTools.loadRSAPrivateKey(filename);
            
            logger.info(result.toString());

            assertNotNull(result);
        }
        finally {

            // Files.delete(java.nio.file.Paths.get(filename));
        }        
    }
}
