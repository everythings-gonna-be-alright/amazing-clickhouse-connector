//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const requestEvents = `[
  {"event_type":"e2e_requests","user_id":1,"url":"/a","time":210,"ts":"2022-09-24 10:18:32.926","api_key":"client-supplied"},
  {"event_type":"e2e_requests","user_id":2,"url":"/b","time":157,"ts":"2022-09-24 10:18:33.926"},
  {"event_type":"e2e_requests","user_id":3,"url":"/c","time":99,"ts":"2022-09-24 10:18:34.926"},
  {"event_type":"e2e_other","kind":"x"},
  {"user_id":4}
]`

const badRequestScript = `
check() {
  want=$1; shift
  got=$(curl -s -o /dev/null -w '%{http_code}' "$@")
  [ "$got" = "$want" ] || { echo "want $want, got $got: $*"; exit 1; }
  echo "ok $want"
}
EVENT="$URL/api/v1/s2s/event"
BIG=$(head -c 8192 /dev/zero | tr '\0' x)
check 401 --data-binary '{"a":1}' "$EVENT"
check 401 -X GET "$EVENT"
check 401 -H "x-auth-token: not-the-token-not-the-token" --data-binary '{"a":1}' "$EVENT"
check 400 -X POST -H "x-auth-token: $TOKEN" "$EVENT"
check 400 -H "x-auth-token: $TOKEN" --data-binary '{"a":' "$EVENT"
check 400 -H "x-auth-token: $TOKEN" --data-binary '123' "$EVENT"
check 400 -H "x-auth-token: $TOKEN" --data-binary '[null]' "$EVENT"
check 413 -H "x-auth-token: $TOKEN" --data-binary "{\"a\":\"$BIG\"}" "$EVENT"
check 200 "$URL/"
`

func TestIngestRejectsBadRequests(t *testing.T) {
	feature := features.New("the ingest endpoint rejects bad requests").
		Assess("unauthenticated, malformed and oversized requests are refused", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			logs := mustRunJob(ctx, t, cfg, jobSpec{
				name:   "bad-requests",
				image:  curlImage,
				script: badRequestScript,
				env:    map[string]string{"URL": connectorURL, "TOKEN": authToken},
			})
			t.Log(strings.TrimSpace(logs))
			return ctx
		}).
		Assess("nothing was written for them", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if tables := connectorTables(ctx, t, cfg); len(tables) != 0 {
				t.Fatalf("expected no tables yet, got %v", tables)
			}
			return ctx
		}).Feature()
	testenv.Test(t, feature)
}

func TestEventsReachClickHouse(t *testing.T) {
	feature := features.New("events travel server -> valkey -> consumer -> ClickHouse").
		Assess("events are stored in a table per event_type", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			postEvents(ctx, t, cfg, requestEvents)

			expectRows(ctx, t, cfg, "SELECT count() AS n FROM e2e_requests", 3)
			expectRows(ctx, t, cfg, "SELECT count() AS n FROM e2e_other", 1)
			expectRows(ctx, t, cfg, "SELECT count() AS n FROM `default`", 1)
			return ctx
		}).
		Assess("column types follow the payload and the configured overrides", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			types := columnTypes(ctx, t, cfg, "e2e_requests")
			want := map[string]string{
				"user_id":             "Float64",
				"url":                 "String",
				"time":                "Float64",
				"ts":                  "String",
				"api_key":             "String",
				"eventn_ctx_event_id": "String",
			}
			for column, typ := range want {
				if types[column] != typ {
					t.Errorf("column %s: got %q, want %q (all: %v)", column, types[column], typ, types)
				}
			}
			if !strings.HasPrefix(types["_timestamp"], "DateTime64") {
				t.Errorf("column _timestamp: got %q, want DateTime64", types["_timestamp"])
			}
			return ctx
		}).
		Assess("connector-owned columns cannot be set by clients", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			expectRows(ctx, t, cfg, "SELECT count() AS n FROM e2e_requests WHERE api_key != ''", 0)
			expectRows(ctx, t, cfg, "SELECT uniqExact(eventn_ctx_event_id) AS n FROM e2e_requests", 3)
			return ctx
		}).
		Assess("an event with a new field alters the table", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			postEvents(ctx, t, cfg, `{"event_type":"e2e_requests","user_id":5,"url":"/d","time":50,"ts":"2022-09-24 10:18:35.926","new_field":"hello"}`)

			expectRows(ctx, t, cfg, "SELECT count() AS n FROM e2e_requests", 4)
			expectRows(ctx, t, cfg, "SELECT count() AS n FROM e2e_requests WHERE new_field = 'hello'", 1)
			if got := columnTypes(ctx, t, cfg, "e2e_requests")["new_field"]; got != "String" {
				t.Errorf("new_field: got %q, want String", got)
			}
			return ctx
		}).Feature()
	testenv.Test(t, feature)
}

func TestUnsafeEventsAreDropped(t *testing.T) {
	const unsafeEvents = "[" +
		"{\"event_type\":\"x`; DROP TABLE e2e_requests; --\",\"a\":1}," +
		`{"event_type":"e2e_unsafe","a b":1},` +
		`{"event_type":"e2e_unsafe","1a":1},` +
		`{"event_type":"e2e_unsafe","a String) ENGINE=Memory --":1},` +
		`{"event_type":7,"a":1},` +
		`{"event_type":"e2e_after_unsafe","ok":1}` +
		"]"

	feature := features.New("events with unsafe names never reach ClickHouse").
		Assess("they are dropped and the pipeline keeps working", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			postEvents(ctx, t, cfg, unsafeEvents)

			expectRows(ctx, t, cfg, "SELECT count() AS n FROM e2e_after_unsafe", 1)
			eventually(t, time.Minute, func() (bool, string) {
				dropped := 0
				for _, pod := range podsWithPrefix(ctx, t, cfg, consumerDeployment+"-") {
					dropped += strings.Count(podLogs(ctx, t, cfg, pod.Name, "clickhouse-connector"), "dropping event")
				}
				return dropped >= 5, fmt.Sprintf("consumers logged %d dropped events, want 5", dropped)
			})
			return ctx
		}).
		Assess("no table was created for them and existing data is intact", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			tables := connectorTables(ctx, t, cfg)
			want := []string{"default", "e2e_after_unsafe", "e2e_other", "e2e_requests"}
			if !slices.Equal(tables, want) {
				t.Fatalf("tables = %v, want %v", tables, want)
			}
			expectRows(ctx, t, cfg, "SELECT count() AS n FROM e2e_requests", 4)
			return ctx
		}).Feature()
	testenv.Test(t, feature)
}

func TestQueueBuffersWhileConsumerIsDown(t *testing.T) {
	const events = `[
  {"event_type":"e2e_buffered","n":1},
  {"event_type":"e2e_buffered","n":2},
  {"event_type":"e2e_buffered","n":3},
  {"event_type":"e2e_buffered","n":4},
  {"event_type":"e2e_buffered","n":5}
]`

	feature := features.New("the queue buffers events while no consumer runs").
		Assess("events wait in valkey", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			scaleDeployment(ctx, t, cfg, consumerDeployment, 0)
			postEvents(ctx, t, cfg, events)

			if got := queueLength(ctx, t, cfg); got != 5 {
				t.Fatalf("queue length = %d, want 5", got)
			}
			if slices.Contains(connectorTables(ctx, t, cfg), "e2e_buffered") {
				t.Fatal("table e2e_buffered exists although no consumer ran")
			}
			return ctx
		}).
		Assess("they are written once a consumer is back", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			scaleDeployment(ctx, t, cfg, consumerDeployment, 1)

			expectRows(ctx, t, cfg, "SELECT count() AS n FROM e2e_buffered", 5)
			eventually(t, time.Minute, func() (bool, string) {
				got := queueLength(ctx, t, cfg)
				return got == 0, fmt.Sprintf("queue length = %d, want 0", got)
			})
			return ctx
		}).Feature()
	testenv.Test(t, feature)
}

func TestWorkloadsAreHardened(t *testing.T) {
	feature := features.New("running pods follow the hardening baseline").
		Assess("security contexts are restrictive", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			pods, err := clientset(t, cfg).CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/instance=" + releaseName})
			if err != nil {
				t.Fatalf("list pods: %v", err)
			}
			checked := 0
			for _, pod := range pods.Items {
				if _, isJob := pod.Labels["batch.kubernetes.io/job-name"]; isJob {
					continue
				}
				checked++
				assertHardened(t, pod)
			}
			if checked < 3 {
				t.Fatalf("expected at least server, consumer and valkey pods, checked %d", checked)
			}
			return ctx
		}).Feature()
	testenv.Test(t, feature)
}

func TestValkeyIsLockedDown(t *testing.T) {
	feature := features.New("valkey only serves authenticated connector traffic").
		Assess("authentication is required and dangerous commands are denied", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			logs := mustRunJob(ctx, t, cfg, jobSpec{
				name:   "valkey-acl",
				image:  valkeyImage(ctx, t, cfg),
				script: valkeyACLScript,
				env:    map[string]string{"HOST": valkeyService},
				// REDISCLI_AUTH keeps the password out of the command line.
				secretEnv: map[string]string{"REDISCLI_AUTH": "redis-password"},
				labels:    connectorLabels,
			})
			t.Log(strings.TrimSpace(logs))
			return ctx
		}).
		Assess("pods outside the release cannot connect", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			logs := mustRunJob(ctx, t, cfg, jobSpec{
				name:  "valkey-outsider",
				image: curlImage,
				script: `
if nc -z -w 3 "$HOST" 6379; then echo "valkey is reachable from a pod that is not part of the release"; exit 1; fi
echo blocked
`,
				env: map[string]string{"HOST": valkeyService},
			})
			t.Log(strings.TrimSpace(logs))
			return ctx
		}).Feature()
	testenv.Test(t, feature)
}

func TestMetricsAreExposed(t *testing.T) {
	feature := features.New("components expose Prometheus metrics").
		Assess("server and consumer serve /metrics", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			for deployment, metric := range map[string]string{
				serverDeployment:   "go_goroutines",
				consumerDeployment: "clickhouse_connector_documents_processed",
			} {
				pods := podsWithPrefix(ctx, t, cfg, deployment+"-")
				if len(pods) == 0 {
					t.Fatalf("no pods for %s", deployment)
				}
				eventually(t, time.Minute, func() (bool, string) {
					raw, err := clientset(t, cfg).CoreV1().RESTClient().Get().
						AbsPath("/api/v1/namespaces", namespace, "pods", pods[0].Name+":6001", "proxy", "metrics").
						DoRaw(ctx)
					return err == nil && strings.Contains(string(raw), metric), fmt.Sprintf("%s: metric %s missing (err: %v)", pods[0].Name, metric, err)
				})
			}
			return ctx
		}).Feature()
	testenv.Test(t, feature)
}

const valkeyACLScript = `
unauth=$(env -u REDISCLI_AUTH valkey-cli -h "$HOST" PING 2>&1 || true)
echo "$unauth" | grep -q NOAUTH || { echo "unauthenticated access was not refused: $unauth"; exit 1; }
[ "$(valkey-cli -h "$HOST" PING)" = PONG ] || { echo "authenticated PING failed"; exit 1; }
for cmd in "FLUSHALL" "FLUSHDB" "EVAL return-1 0" "SCRIPT FLUSH" "FUNCTION FLUSH"; do
  res=$(valkey-cli -h "$HOST" $cmd 2>&1 || true)
  echo "$res" | grep -q NOPERM || { echo "$cmd was not denied: $res"; exit 1; }
done
echo "acl ok"
`

// queueLength returns the number of events waiting in the connector's queue.
func queueLength(ctx context.Context, t *testing.T, cfg *envconf.Config) int {
	t.Helper()
	logs := mustRunJob(ctx, t, cfg, jobSpec{
		name:      "queue-length",
		image:     valkeyImage(ctx, t, cfg),
		script:    `valkey-cli -h "$HOST" LLEN "$QUEUE"`,
		env:       map[string]string{"HOST": valkeyService, "QUEUE": queueName},
		secretEnv: map[string]string{"REDISCLI_AUTH": "redis-password"},
		labels:    connectorLabels,
	})
	n, err := strconv.Atoi(strings.TrimSpace(logs))
	if err != nil {
		t.Fatalf("parse queue length from %q: %v", logs, err)
	}
	return n
}

// valkeyImage returns the image the chart deployed, so the test client always
// matches the server version.
func valkeyImage(ctx context.Context, t *testing.T, cfg *envconf.Config) string {
	t.Helper()
	for _, pod := range podsWithPrefix(ctx, t, cfg, valkeyService+"-") {
		for _, c := range pod.Spec.Containers {
			if c.Name == valkeyService {
				return c.Image
			}
		}
	}
	t.Fatal("valkey pod not found")
	return ""
}

func assertHardened(t *testing.T, pod corev1.Pod) {
	t.Helper()
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Errorf("%s: service account token is mounted", pod.Name)
	}
	podCtx := pod.Spec.SecurityContext
	if podCtx == nil || podCtx.SeccompProfile == nil || podCtx.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("%s: seccomp profile is not RuntimeDefault", pod.Name)
	}
	containers := append(slices.Clone(pod.Spec.InitContainers), pod.Spec.Containers...)
	for _, c := range containers {
		sc := c.SecurityContext
		name := pod.Name + "/" + c.Name
		if sc == nil {
			t.Errorf("%s: no security context", name)
			continue
		}
		if sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
			t.Errorf("%s: root filesystem is writable", name)
		}
		if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
			t.Errorf("%s: privilege escalation is allowed", name)
		}
		if sc.Privileged != nil && *sc.Privileged {
			t.Errorf("%s: privileged", name)
		}
		if sc.Capabilities == nil || !slices.Contains(sc.Capabilities.Drop, "ALL") {
			t.Errorf("%s: capabilities are not dropped", name)
		}
		nonRoot := (sc.RunAsNonRoot != nil && *sc.RunAsNonRoot) || (podCtx != nil && podCtx.RunAsNonRoot != nil && *podCtx.RunAsNonRoot)
		if !nonRoot {
			t.Errorf("%s: may run as root", name)
		}
		if strings.HasSuffix(strings.SplitN(c.Image, "@", 2)[0], ":latest") {
			t.Errorf("%s: uses the latest tag (%s)", name, c.Image)
		}
	}
}
