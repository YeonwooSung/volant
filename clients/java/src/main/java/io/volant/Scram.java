package io.volant;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Client-side SCRAM-SHA-256 proof computation (v0.46).
 *
 * <p>Matches {@code crates/volant-client/src/scram.rs} (no channel binding).
 */
final class Scram {
    private Scram() {}

    static String generateClientNonce() {
        byte[] buf = new byte[16];
        new SecureRandom().nextBytes(buf);
        return Base64.getEncoder().encodeToString(buf).replace(',', 'A');
    }

    static final class Proof {
        final byte[] clientProof;
        final byte[] serverSignature;

        Proof(byte[] clientProof, byte[] serverSignature) {
            this.clientProof = clientProof;
            this.serverSignature = serverSignature;
        }
    }

    static Proof clientProofAndServerSig(
            String username,
            String password,
            String clientNonce,
            String combinedNonce,
            byte[] salt,
            int iterations) {
        try {
            byte[] salted = pbkdf2(password, salt, iterations);
            byte[] clientKey = hmacSha256(salted, "Client Key".getBytes(StandardCharsets.UTF_8));
            byte[] storedKey = sha256(clientKey);
            byte[] serverKey = hmacSha256(salted, "Server Key".getBytes(StandardCharsets.UTF_8));
            String authMessage = buildAuthMessage(username, clientNonce, combinedNonce, salt, iterations);
            byte[] authBytes = authMessage.getBytes(StandardCharsets.UTF_8);
            byte[] clientSignature = hmacSha256(storedKey, authBytes);
            byte[] proof = new byte[32];
            for (int i = 0; i < 32; i++) {
                proof[i] = (byte) (clientKey[i] ^ clientSignature[i]);
            }
            byte[] serverSig = hmacSha256(serverKey, authBytes);
            return new Proof(proof, serverSig);
        } catch (GeneralSecurityException e) {
            throw new ProtocolException("scram crypto failed: " + e.getMessage(), e);
        }
    }

    static boolean signaturesEqual(byte[] a, byte[] b) {
        return MessageDigest.isEqual(a == null ? new byte[0] : a, b == null ? new byte[0] : b);
    }

    private static byte[] pbkdf2(String password, byte[] salt, int iterations)
            throws GeneralSecurityException {
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, 256);
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        try {
            return factory.generateSecret(spec).getEncoded();
        } finally {
            spec.clearPassword();
        }
    }

    private static byte[] hmacSha256(byte[] key, byte[] data) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data);
    }

    private static byte[] sha256(byte[] data) throws GeneralSecurityException {
        return MessageDigest.getInstance("SHA-256").digest(data);
    }

    private static String buildAuthMessage(
            String username, String clientNonce, String combinedNonce, byte[] salt, int iterations) {
        String clientFirstBare = "n=" + username + ",r=" + clientNonce;
        String serverFirst =
                "r=" + combinedNonce + ",s=" + Base64.getEncoder().encodeToString(salt) + ",i=" + iterations;
        String clientFinalWoProof = "c=biws,r=" + combinedNonce;
        return clientFirstBare + "," + serverFirst + "," + clientFinalWoProof;
    }
}
