package cl.bithaus.medium.utils;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

/**
 * Utility class for crypto operations
 */
public class CryptoTools {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    
    /**
     * Reads a x509 certificate from a file
     * @param filename The file name
     * @return The certificate
     * @throws IOException
     * @throws CertificateException
     */
    public static X509Certificate loadX509Certificate(String filename) throws IOException, CertificateException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        FileInputStream in = new FileInputStream(filename);
        X509Certificate cert = (X509Certificate) cf.generateCertificate(in);
        in.close();
        return cert;
    }

    /**
     * Reads a RSA private key from a file in PKCS1 format
     * @param filename The file name
     * @return The private key
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeySpecException
     */
    public static RSAPrivateKey loadRSAPrivateKey(String filename) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
     
        KeyFactory factory = KeyFactory.getInstance("RSA");

        try (FileReader keyReader = new FileReader(filename);
        PemReader pemReader = new PemReader(keyReader)) {

            PemObject pemObject = pemReader.readPemObject();
            byte[] content = pemObject.getContent();
            PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(content);
            return (RSAPrivateKey) factory.generatePrivate(privKeySpec);
        }                 
    }
}
