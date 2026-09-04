package volant

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"
	"strings"
)

// generateClientNonce returns 16 random bytes, standard Base64, ',' → 'A'.
func generateClientNonce() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return strings.ReplaceAll(base64.StdEncoding.EncodeToString(buf), ",", "A"), nil
}

// ClientProofAndServerSig matches crates/volant-client/src/scram.rs (SHA-256).
func ClientProofAndServerSig(username, password, clientNonce, combinedNonce string, salt []byte, iterations uint32) (proof, serverSig []byte, err error) {
	return clientProofAndServerSig(sha256.New, 32, username, password, clientNonce, combinedNonce, salt, iterations)
}

// ClientProofAndServerSigSHA512 is the v0.238 SHA-512 proof (64-byte digest).
func ClientProofAndServerSigSHA512(username, password, clientNonce, combinedNonce string, salt []byte, iterations uint32) (proof, serverSig []byte, err error) {
	return clientProofAndServerSig(sha512.New, 64, username, password, clientNonce, combinedNonce, salt, iterations)
}

func clientProofAndServerSig(h func() hash.Hash, dklen int, username, password, clientNonce, combinedNonce string, salt []byte, iterations uint32) (proof, serverSig []byte, err error) {
	if iterations == 0 {
		return nil, nil, fmt.Errorf("scram iterations must be > 0")
	}
	salted := pbkdf2HMAC(h, []byte(password), salt, int(iterations), dklen)
	clientKey := hmacHash(h, salted, []byte("Client Key"))
	storedKey := digestHash(h, clientKey)
	serverKey := hmacHash(h, salted, []byte("Server Key"))
	authMessage := buildAuthMessage(username, clientNonce, combinedNonce, salt, iterations)
	clientSig := hmacHash(h, storedKey, []byte(authMessage))
	proof = make([]byte, dklen)
	for i := 0; i < dklen; i++ {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	serverSig = hmacHash(h, serverKey, []byte(authMessage))
	return proof, serverSig, nil
}

func pbkdf2HMAC(h func() hash.Hash, password, salt []byte, iterations, keyLen int) []byte {
	hLen := h().Size()
	nBlocks := (keyLen + hLen - 1) / hLen
	out := make([]byte, 0, nBlocks*hLen)
	var blockNum [4]byte
	for i := 1; i <= nBlocks; i++ {
		binary.BigEndian.PutUint32(blockNum[:], uint32(i))
		u := hmacHash(h, password, append(append([]byte{}, salt...), blockNum[:]...))
		t := append([]byte(nil), u...)
		for j := 1; j < iterations; j++ {
			u = hmacHash(h, password, u)
			for k := range t {
				t[k] ^= u[k]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

func hmacHash(h func() hash.Hash, key, data []byte) []byte {
	m := hmac.New(h, key)
	_, _ = m.Write(data)
	return m.Sum(nil)
}

func digestHash(h func() hash.Hash, data []byte) []byte {
	d := h()
	_, _ = d.Write(data)
	return d.Sum(nil)
}

func buildAuthMessage(username, clientNonce, combinedNonce string, salt []byte, iterations uint32) string {
	clientFirstBare := "n=" + username + ",r=" + clientNonce
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", combinedNonce, base64.StdEncoding.EncodeToString(salt), iterations)
	clientFinalWoProof := "c=biws,r=" + combinedNonce
	return clientFirstBare + "," + serverFirst + "," + clientFinalWoProof
}
