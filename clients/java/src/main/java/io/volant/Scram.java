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
 * Client-side SCRAM proof computation (v0.46 SHA-256; v0.238 SHA-512).
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
        return proofFor(
                username,
                password,
                clientNonce,
                combinedNonce,
                salt,
                iterations,
                256,
                "PBKDF2WithHmacSHA256",
                "HmacSHA256",
                "SHA-256");
    }

    static Proof clientProofAndServerSigSha512(
            String username,
            String password,
            String clientNonce,
            String combinedNonce,
            byte[] salt,
            int iterations) {
        return proofFor(
                username,
                password,
                clientNonce,
                combinedNonce,
                salt,
                iterations,
                512,
                "PBKDF2WithHmacSHA512",
                "HmacSHA512",
                "SHA-512");
    }

    private static Proof proofFor(
            String username,
            String password,
            String clientNonce,
            String combinedNonce,
            byte[] salt,
            int iterations,
            int bits,
            String pbkdfAlg,
            String hmacAlg,
            String digestAlg) {
        try {
            byte[] salted = pbkdf2(password, salt, iterations, bits, pbkdfAlg);
            byte[] clientKey = hmac(hmacAlg, salted, "Client Key".getBytes(StandardCharsets.UTF_8));
            byte[] storedKey = digest(digestAlg, clientKey);
            byte[] serverKey = hmac(hmacAlg, salted, "Server Key".getBytes(StandardCharsets.UTF_8));
            String authMessage = buildAuthMessage(username, clientNonce, combinedNonce, salt, iterations);
            byte[] authBytes = authMessage.getBytes(StandardCharsets.UTF_8);
            byte[] clientSignature = hmac(hmacAlg, storedKey, authBytes);
            byte[] proof = new byte[bits / 8];
            for (int i = 0; i < proof.length; i++) {
                proof[i] = (byte) (clientKey[i] ^ clientSignature[i]);
            }
            byte[] serverSig = hmac(hmacAlg, serverKey, authBytes);
            return new Proof(proof, serverSig);
        } catch (GeneralSecurityException e) {
            throw new ProtocolException("scram crypto failed: " + e.getMessage(), e);
        }
    }

    static boolean signaturesEqual(byte[] a, byte[] b) {
        return MessageDigest.isEqual(a == null ? new byte[0] : a, b == null ? new byte[0] : b);
    }

    private static byte[] pbkdf2(String password, byte[] salt, int iterations, int bits, String alg)
            throws GeneralSecurityException {
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, bits);
        SecretKeyFactory factory = SecretKeyFactory.getInstance(alg);
        try {
            return factory.generateSecret(spec).getEncoded();
        } finally {
            spec.clearPassword();
        }
    }

    private static byte[] hmac(String alg, byte[] key, byte[] data) throws GeneralSecurityException {
        Mac mac = Mac.getInstance(alg);
        mac.init(new SecretKeySpec(key, alg));
        return mac.doFinal(data);
    }

    private static byte[] digest(String alg, byte[] data) throws GeneralSecurityException {
        return MessageDigest.getInstance(alg).digest(data);
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
