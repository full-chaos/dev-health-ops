package devhealth_test

import (
	"strings"
	"testing"
)

const (
	txnSurgeKey     = "goWorkers.pgbouncer.transaction.rollingUpdate"
	queuePooler     = "a-dev-health-go-pgbouncer-queue-session"
	coordinatorPool = "a-dev-health-go-pgbouncer-coordinator-session"
	txnPoolSizeKey  = "goWorkers.pgbouncer.transaction.poolSize"
	prodShapeBudget = "97"
)

// Prod shape after CHAOS-11542, budget 97: queue-session 36, coordinator 10,
// KEDA 4, auth-service (2 + 1 surge) x 8 = 24. The surge pod of the transaction
// pooler opens EVERY transaction pool again, so the transaction term counts
// twice: 2 x (T + 4) + 36 + 10 + 24.
// sessionNoSurge pins both session poolers to maxSurge 0 so a test that is
// about the transaction term keeps its sum: an UNSET session pooler counts one
// surge pod (CHAOS-11583).
var sessionNoSurge = []string{
	"--set-json", sessionRollingUpdateKey("queueSession") + `={"maxSurge":0,"maxUnavailable":1}`,
	"--set-json", sessionRollingUpdateKey("coordinatorSession") + `={"maxSurge":0,"maxUnavailable":1}`,
}

func sessionRollingUpdateKey(pooler string) string {
	return "goWorkers.pgbouncer." + pooler + ".rollingUpdate"
}

func prodShapeAt(t int, extra ...string) []string {
	args := []string{
		"--set", txnPoolSizeKey + "=" + itoa(t),
		"--set", "goWorkers.pgbouncer.queueSession.poolSize=36",
		"--set", "goWorkers.pgbouncer.coordinatorSession.poolSize=10",
		"--set", "authService.database.connectionBudget=" + prodShapeBudget,
	}
	args = append(args, sessionNoSurge...)
	return authWith(append(args, extra...)...)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

func TestTransactionSurgeIsCountedT16FailsNamingTheSurge(t *testing.T) {
	_, out, err := renderAuth(t, prodShapeAt(16)...)
	// 2 x (16 + 4) + 36 + 10 + 24 = 110
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "110") ||
		!strings.Contains(out, "transaction pooler") || !strings.Contains(out, "surge pod") {
		t.Fatalf("T 16 must fail 97 with the named message, the sum 110 and the transaction surge: err=%v out=%.600s", err, out)
	}
}

func TestTransactionSurgeIsCountedT9PassesAndT10Fails(t *testing.T) {
	// 2 x (9 + 4) + 70 = 96 of 97.
	if _, out, err := renderAuth(t, prodShapeAt(9)...); err != nil {
		t.Fatalf("T 9 (96 of 97) must render: %v\n%.500s", err, out)
	}
	// 2 x (10 + 4) + 70 = 98.
	_, out, err := renderAuth(t, prodShapeAt(10)...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "98") {
		t.Fatalf("T 10 (98 of 97) must fail with the named message: err=%v out=%.500s", err, out)
	}
}

func TestTransactionSurgeMaxSurgeZeroOverrideDropsTheCount(t *testing.T) {
	zero := []string{"--set-json", txnSurgeKey + `={"maxSurge":0,"maxUnavailable":1}`}
	// T 16, no surge pod: 20 + 36 + 10 + 24 = 90.
	docs, out, err := renderAuth(t, prodShapeAt(16, zero...)...)
	if err != nil {
		t.Fatalf("maxSurge 0 with T 16 counts 90 and must render at 97: %v\n%.500s", err, out)
	}
	ru := at(authDoc(docs, "Deployment", transactionPooler), "spec", "strategy", "rollingUpdate")
	if at(ru, "maxSurge") != float64(0) && at(ru, "maxSurge") != 0 {
		t.Fatalf("the transaction Deployment must carry maxSurge 0, got %v", ru)
	}
	if at(ru, "maxUnavailable") != float64(1) && at(ru, "maxUnavailable") != 1 {
		t.Fatalf("the transaction Deployment must carry maxUnavailable 1, got %v", ru)
	}
	// 90 is the boundary: budget 89 fails.
	args := append(prodShapeAt(16, zero...), "--set", "authService.database.connectionBudget=89")
	if _, out, err := renderAuth(t, args...); err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "90") {
		t.Fatalf("90 over a budget of 89 must fail: err=%v out=%.400s", err, out)
	}
}

func TestTransactionSurgeTwoPodsCountThreeTimes(t *testing.T) {
	two := []string{"--set-json", txnSurgeKey + `={"maxSurge":2}`}
	// 3 x (9 + 4) + 70 = 109.
	_, out, err := renderAuth(t, prodShapeAt(9, two...)...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "109") {
		t.Fatalf("maxSurge 2 counts three pods (109): err=%v out=%.500s", err, out)
	}
}

func TestTransactionDefaultRendersNoStrategyAndCountsOneSurgePod(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.300s", err, out)
	}
	for _, name := range []string{transactionPooler, queuePooler, coordinatorPool} {
		if s := at(authDoc(docs, "Deployment", name), "spec", "strategy"); s != nil {
			t.Errorf("%s must render no strategy key by default, got %v", name, s)
		}
		if r := at(authDoc(docs, "Deployment", name), "spec", "replicas"); r != float64(1) && r != 1 {
			t.Errorf("%s keeps one replica, got %v", name, r)
		}
	}
}

func TestTransactionRollingUpdateWithBudgetOffRendersAnything(t *testing.T) {
	zero := []string{"--set-json", txnSurgeKey + `={"maxSurge":0,"maxUnavailable":1}`}
	if _, out, err := renderAuth(t, authWith(append(zero, "--set", "authService.database.connectionBudget=0")...)...); err != nil {
		t.Fatalf("budget 0 leaves the render unchanged: %v\n%.300s", err, out)
	}
}

func TestTransactionRollingUpdateIsValidated(t *testing.T) {
	bad := map[string]string{
		"percentage":            `{"maxSurge":"25%"}`,
		"negative":              `{"maxSurge":-1}`,
		"fraction":              `{"maxSurge":0.5}`,
		"too large":             `{"maxSurge":11}`,
		"both zero":             `{"maxSurge":0,"maxUnavailable":0}`,
		"surge 0 without unavl": `{"maxSurge":0}`,
		"unknown key":           `{"minReady":1}`,
	}
	for name, v := range bad {
		set := []string{"--set-json", txnSurgeKey + "=" + v}
		if _, out, err := renderAuth(t, authWith(set...)...); err == nil || !strings.Contains(out, "rollingUpdate") {
			t.Errorf("%s: want a refusal naming rollingUpdate: err=%v out=%.300s", name, err, out)
		}
		args := authWith(append(set, "--set", "authService.database.connectionBudget=97", "--skip-schema-validation")...)
		if name == "unknown key" {
			continue
		}
		if _, out, err := renderAuth(t, args...); err == nil || !strings.Contains(out, "rollingUpdate") {
			t.Errorf("%s without the schema: want the helper refusal naming rollingUpdate: err=%v out=%.300s", name, err, out)
		}
	}
}
