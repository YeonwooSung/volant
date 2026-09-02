package volant

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
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

// ClientProofAndServerSig matches crates/volant-client/src/scram.rs.
func ClientProofAndServerSig(username, password, clientNonce, combinedNonce string, salt []byte, iterations uint32) (proof, serverSig []byte, err error) {
	if iterations == 0 {
		return nil, nil, fmt.Errorf("scram iterations must be > 0")
	}
	salted := pbkdf2HMACSHA256([]byte(password), salt, int(iterations), 32)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256Sum(clientKey)
	serverKey := hmacSHA256(salted, []byte("Server Key"))
	authMessage := buildAuthMessage(username, clientNonce, combinedNonce, salt, iterations)
	clientSig := hmacSHA256(storedKey, []byte(authMessage))
	proof = make([]byte, 32)
	for i := 0; i < 32; i++ {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	serverSig = hmacSHA256(serverKey, []byte(authMessage))
	return proof, serverSig, nil
}

func pbkdf2HMACSHA256(password, salt []byte, iterations, keyLen int) []byte {
	const hLen = sha256.Size
	nBlocks := (keyLen + hLen - 1) / hLen
	out := make([]byte, 0, nBlocks*hLen)
	var blockNum [4]byte
	for i := 1; i <= nBlocks; i++ {
		binary.BigEndian.PutUint32(blockNum[:], uint32(i))
		u := hmacSHA256(password, append(append([]byte{}, salt...), blockNum[:]...))
		t := append([]byte(nil), u...)
		for j := 1; j < iterations; j++ {
			u = hmacSHA256(password, u)
			for k := range t {
				t[k] ^= u[k]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	_, _ = m.Write(data)
	return m.Sum(nil)
}

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func buildAuthMessage(username, clientNonce, combinedNonce string, salt []byte, iterations uint32) string {
	clientFirstBare := "n=" + username + ",r=" + clientNonce
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", combinedNonce, base64.StdEncoding.EncodeToString(salt), iterations)
	clientFinalWoProof := "c=biws,r=" + combinedNonce
	return clientFirstBare + "," + serverFirst + "," + clientFinalWoProof
}
