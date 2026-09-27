package processlicense

import (
	"crypto/ed25519"
	"crypto/sha512"
	"encoding/binary"
	"math/big"
	"testing"
)

// A slow, test-only edwards25519 in affine coordinates over math/big: enough
// to forge the signatures Go's ed25519.Verify accepts and libsodium refuses
// (small-order public keys with an ordinary R), which the stdlib cannot
// build because it exposes no point arithmetic.

var (
	fieldP  = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(19))
	curveD  = mulMod(big.NewInt(-121665), new(big.Int).ModInverse(big.NewInt(121666), fieldP))
	sqrtM1  = new(big.Int).Exp(big.NewInt(2), new(big.Int).Div(new(big.Int).Sub(fieldP, big.NewInt(1)), big.NewInt(4)), fieldP)
	basePtY = mulMod(big.NewInt(4), new(big.Int).ModInverse(big.NewInt(5), fieldP))
)

type edPoint struct{ x, y *big.Int }

func mulMod(a, b *big.Int) *big.Int {
	out := new(big.Int).Mul(a, b)
	return out.Mod(out, fieldP)
}

func identityPoint() edPoint { return edPoint{big.NewInt(0), big.NewInt(1)} }

func (p edPoint) add(q edPoint) edPoint {
	x1y2, x2y1 := mulMod(p.x, q.y), mulMod(q.x, p.y)
	y1y2, x1x2 := mulMod(p.y, q.y), mulMod(p.x, q.x)
	dxy := mulMod(curveD, mulMod(x1x2, y1y2))
	one := big.NewInt(1)
	xDen := new(big.Int).ModInverse(new(big.Int).Mod(new(big.Int).Add(one, dxy), fieldP), fieldP)
	yDen := new(big.Int).ModInverse(new(big.Int).Mod(new(big.Int).Sub(one, dxy), fieldP), fieldP)
	x := mulMod(new(big.Int).Add(x1y2, x2y1), xDen)
	y := mulMod(new(big.Int).Add(y1y2, x1x2), yDen)
	return edPoint{x, y}
}

func (p edPoint) neg() edPoint {
	return edPoint{new(big.Int).Mod(new(big.Int).Neg(p.x), fieldP), new(big.Int).Set(p.y)}
}

func (p edPoint) mul(k *big.Int) edPoint {
	result := identityPoint()
	for i := k.BitLen() - 1; i >= 0; i-- {
		result = result.add(result)
		if k.Bit(i) == 1 {
			result = result.add(p)
		}
	}
	return result
}

// encode is RFC 8032 point encoding: y little-endian, x's parity in bit 255.
func (p edPoint) encode() []byte {
	out := reverse(p.y.FillBytes(make([]byte, 32)))
	out[31] |= byte(p.x.Bit(0)) << 7
	return out
}

// decode is RFC 8032 point decoding, accepting a non-canonical y as Go does.
func decodePoint(encoded []byte) (edPoint, bool) {
	raw := append([]byte(nil), encoded...)
	sign := raw[31] >> 7
	raw[31] &= 0x7f
	y := new(big.Int).Mod(new(big.Int).SetBytes(reverse(raw)), fieldP)
	y2 := mulMod(y, y)
	u := new(big.Int).Mod(new(big.Int).Sub(y2, big.NewInt(1)), fieldP)
	v := new(big.Int).Mod(new(big.Int).Add(mulMod(curveD, y2), big.NewInt(1)), fieldP)
	x2 := mulMod(u, new(big.Int).ModInverse(v, fieldP))
	x := new(big.Int).Exp(x2, new(big.Int).Div(new(big.Int).Add(fieldP, big.NewInt(3)), big.NewInt(8)), fieldP)
	if mulMod(x, x).Cmp(x2) != 0 {
		x = mulMod(x, sqrtM1)
	}
	if mulMod(x, x).Cmp(x2) != 0 {
		return edPoint{}, false
	}
	if x.Sign() == 0 && sign == 1 {
		return edPoint{}, false
	}
	if byte(x.Bit(0)) != sign {
		x.Sub(fieldP, x)
	}
	return edPoint{x, y}, true
}

func basePoint() edPoint {
	point, _ := decodePoint(reverse(basePtY.FillBytes(make([]byte, 32))))
	return point
}

// forgeUnderSmallOrderKey signs message for the small-order public key
// encoded as key, with an ordinary (not small-order) R: for S = 1, 2, ...
// and each j in 0..7, R = [S]B - [j]A verifies whenever h(R, A, M) mod L
// is j modulo 8 (A's order divides 8, so [h]A = [j]A). ok is false when key
// does not decode.
func forgeUnderSmallOrderKey(key, message []byte) ([]byte, bool) {
	a, ok := decodePoint(key)
	if !ok {
		return nil, false
	}
	b := basePoint()
	for s := int64(1); s < 4096; s++ {
		sB := b.mul(big.NewInt(s))
		for j := int64(0); j < 8; j++ {
			r := sB.add(a.mul(big.NewInt(j)).neg()).encode()
			hash := sha512.New()
			hash.Write(r)
			hash.Write(key)
			hash.Write(message)
			// The verifier multiplies A by h reduced mod L, so [h]A is
			// [(h mod L) mod 8]A.
			h := new(big.Int).SetBytes(reverse(hash.Sum(nil)))
			h.Mod(h, groupOrder)
			if new(big.Int).Mod(h, big.NewInt(8)).Int64() != j {
				continue
			}
			scalar := make([]byte, 32)
			binary.LittleEndian.PutUint64(scalar, uint64(s))
			return append(r, scalar...), true
		}
	}
	return nil, false
}

// The arithmetic is checked against crypto/ed25519 itself, so a forgery
// test cannot pass on a broken helper.
func TestEdwardsHelperAgreesWithCryptoEd25519(t *testing.T) {
	b := basePoint()
	if got := b.mul(groupOrder); got.x.Sign() != 0 || got.y.Cmp(big.NewInt(1)) != 0 {
		t.Fatal("[L]B is not the identity")
	}
	for seed := byte(0); seed < 4; seed++ {
		private := ed25519.NewKeyFromSeed(append(make([]byte, 31), seed))
		digest := sha512.Sum512(private.Seed())
		scalar := digest[:32]
		scalar[0] &= 248
		scalar[31] &= 127
		scalar[31] |= 64
		if got := b.mul(new(big.Int).SetBytes(reverse(scalar))).encode(); string(got) != string(private.Public().(ed25519.PublicKey)) {
			t.Fatalf("seed %d: [a]B encodes differently from crypto/ed25519", seed)
		}
	}
}
