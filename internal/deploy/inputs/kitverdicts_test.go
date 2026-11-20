package inputs

import (
	"strings"
	"testing"
)

// kitVerdicts is `<verdict>\t<key>\t<value>` per line: the verdict of the kit's own
// bash [[ =~ ]] with keyre() (agentkit 7022161ce650 prodroll.sh v3z21) on each value.
// The values sit on the edges of the three keys whose kit regex RE2 cannot compile.
const kitVerdicts = `1	PODSNAP_TARGETS	a
1	PODSNAP_TARGETS	a-b
0	PODSNAP_TARGETS	-a
0	PODSNAP_TARGETS	a-
0	PODSNAP_TARGETS	A
1	PODSNAP_TARGETS	abbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbc
0	PODSNAP_TARGETS	abbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbc
1	PODSNAP_TARGETS	n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16,n17,n18,n19,n20,n21,n22,n23,n24,n25,n26,n27,n28,n29,n30,n31
0	PODSNAP_TARGETS	n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16,n17,n18,n19,n20,n21,n22,n23,n24,n25,n26,n27,n28,n29,n30,n31,n32
0	PODSNAP_TARGETS	a,,b
0	PODSNAP_TARGETS	a,b-
1	PODSNAP_TARGETS	none
0	PODSNAP_TARGETS	a b
0	PODSNAP_TARGETS	
1	PODSNAP_SCALERS	none
1	PODSNAP_SCALERS	none,a
1	PODSNAP_SCALERS	a
1	PODSNAP_SCALERS	n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16,n17,n18,n19,n20,n21,n22,n23,n24,n25,n26,n27,n28,n29,n30,n31
0	PODSNAP_SCALERS	n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16,n17,n18,n19,n20,n21,n22,n23,n24,n25,n26,n27,n28,n29,n30,n31,n32
0	PODSNAP_SCALERS	
1	PODSNAP_SCALERS	a,none
0	PODSNAP_SCALERS	NONE
1	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc
1	IMGROW	Dx/web .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc
0	IMGROW	D/web .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc
1	IMGROW	Deployment/abbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbc .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/abbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbc .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc
0	IMGROW	deployment/web .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/Web .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc
1	IMGROW	Deployment/web .spec[0].x[1].x[2].x[3].x[4].x[5].x[6].x[7].x 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .spec[0].x[1].x[2].x[3].x[4].x[5].x[6].x[7].x[8].x 2c87c3613bca 3c61345532cc
1	IMGROW	Deployment/web .aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 2c87c3613bca 3c61345532cc
1	IMGROW	Deployment/web .a[0]bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .a[0]bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .a[1000] 2c87c3613bca 3c61345532cc
1	IMGROW	Deployment/web .a[999] 2c87c3613bca 3c61345532cc
1	IMGROW	Deployment/web .a[_a] 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .a[a-b] 2c87c3613bca 3c61345532cc
1	IMGROW	Deployment/web .a[aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa] 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .a[aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa] 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web spec.x 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web . 2c87c3613bca 3c61345532cc
1	IMGROW	Deployment/web ._ 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .a[] 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .a[0]] 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .a[[0] 2c87c3613bca 3c61345532cc
1	IMGROW	Deployment/web .env[DB_PASSWORD].value <secret> <none>
1	IMGROW	Deployment/web .spec.template.spec.containers[0].image <secret> <empty>
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image <empty> 3c61345532cc
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca <secret>
1	IMGROW	Deployment/web .spec.template.spec.containers[0].image <none> <none>
1	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca abc$
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca a$b
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca $
1	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
1	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx$
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx$
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc 
0	IMGROW	Deployment/web  .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca a'b
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca a#b
1	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca http://u@h:1/p,q!=+-_.
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image  3c61345532cc
0	IMGROW	Deployment/web .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc x
`

func TestTheMatchersGiveTheKitVerdicts(t *testing.T) {
	n := 0
	for _, row := range strings.Split(strings.TrimSuffix(kitVerdicts, "\n"), "\n") {
		verdict, rest, _ := strings.Cut(row, "\t")
		key, value, _ := strings.Cut(rest, "\t")
		got := targets["dev-health"].keys[key].match(value)
		if got != (verdict == "1") {
			t.Errorf("%s %q (%d bytes): match %v, kit %s", key, value, len(value), got, verdict)
		}
		n++
	}
	if n != 66 {
		t.Fatalf("%d corpus rows, want 66", n)
	}
}
