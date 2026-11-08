//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

const curlImage = "curlimages/curl:8.23.0@sha256:3fb7e13e5d179a845c97d78204f3f1064d1907037f26a3ebf91710571151fd93"

// connectorLabels are the labels the chart's NetworkPolicies admit to Valkey.
var connectorLabels = map[string]string{
	"app.kubernetes.io/name":     "clickhouse-connector",
	"app.kubernetes.io/instance": releaseName,
}

func clientset(t *testing.T, cfg *envconf.Config) *kubernetes.Clientset {
	t.Helper()
	cs, err := kubernetes.NewForConfig(cfg.Client().RESTConfig())
	if err != nil {
		t.Fatalf("kubernetes client: %v", err)
	}
	return cs
}

// eventually polls check until it succeeds or the timeout expires.
func eventually(t *testing.T, timeout time.Duration, check func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := ""
	for {
		ok, msg := check()
		if ok {
			return
		}
		last = msg
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s: %s", timeout, last)
		}
		time.Sleep(2 * time.Second)
	}
}

type jobSpec struct {
	name   string
	image  string
	script string
	env    map[string]string
	// secretEnv maps an env var to a key of the release secret.
	secretEnv map[string]string
	labels    map[string]string
}

// runJob runs a hardened one-shot pod in the release namespace and returns its
// logs and whether it succeeded. Traffic goes through the chart's NetworkPolicies
// like a real in-cluster client would.
func runJob(ctx context.Context, t *testing.T, cfg *envconf.Config, spec jobSpec) (string, bool) {
	t.Helper()

	var envVars []corev1.EnvVar
	for k, v := range spec.env {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}
	for k, key := range spec.secretEnv {
		envVars = append(envVars, corev1.EnvVar{Name: k, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: releaseSecret}, Key: key},
		}})
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{GenerateName: spec.name + "-", Namespace: namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](0),
			TTLSecondsAfterFinished: ptr.To[int32](600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: spec.labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: ptr.To(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr.To(true),
						RunAsUser:      ptr.To[int64](1000),
						RunAsGroup:     ptr.To[int64](1000),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name:    "job",
						Image:   spec.image,
						Command: []string{"sh", "-ec", spec.script},
						Env:     envVars,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							ReadOnlyRootFilesystem:   ptr.To(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
				},
			},
		},
	}

	cs := clientset(t, cfg)
	created, err := cs.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create job %s: %v", spec.name, err)
	}

	var succeeded bool
	eventually(t, 10*time.Minute, func() (bool, string) {
		j, err := cs.BatchV1().Jobs(namespace).Get(ctx, created.Name, metav1.GetOptions{})
		if err != nil {
			return false, err.Error()
		}
		if j.Status.Succeeded > 0 {
			succeeded = true
			return true, ""
		}
		return j.Status.Failed > 0, fmt.Sprintf("job %s still running", created.Name)
	})
	return jobLogs(ctx, t, cs, created.Name), succeeded
}

func mustRunJob(ctx context.Context, t *testing.T, cfg *envconf.Config, spec jobSpec) string {
	t.Helper()
	logs, ok := runJob(ctx, t, cfg, spec)
	if !ok {
		t.Fatalf("job %s failed:\n%s", spec.name, logs)
	}
	return logs
}

func jobLogs(ctx context.Context, t *testing.T, cs *kubernetes.Clientset, job string) string {
	t.Helper()
	pods, err := cs.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + job})
	if err != nil || len(pods.Items) == 0 {
		return fmt.Sprintf("<no logs: %v>", err)
	}
	raw, err := cs.CoreV1().Pods(namespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).DoRaw(ctx)
	if err != nil {
		return fmt.Sprintf("<no logs: %v>", err)
	}
	return string(raw)
}

// postEvents sends payload to the ingest endpoint with a valid token and fails
// unless the connector accepts it.
func postEvents(ctx context.Context, t *testing.T, cfg *envconf.Config, payload string) {
	t.Helper()
	mustRunJob(ctx, t, cfg, jobSpec{
		name:  "post-events",
		image: curlImage,
		script: `
code=$(curl -s -o /dev/null -w '%{http_code}' -H "x-auth-token: $TOKEN" --data-binary "$PAYLOAD" "$URL/api/v1/s2s/event")
[ "$code" = 200 ] || { echo "ingest returned $code"; exit 1; }
`,
		env: map[string]string{"URL": connectorURL, "TOKEN": authToken, "PAYLOAD": payload},
	})
}

// podsWithPrefix returns the pods of the release whose name starts with prefix.
func podsWithPrefix(ctx context.Context, t *testing.T, cfg *envconf.Config, prefix string) []corev1.Pod {
	t.Helper()
	pods, err := clientset(t, cfg).CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	var matched []corev1.Pod
	for _, p := range pods.Items {
		if strings.HasPrefix(p.Name, prefix) && p.DeletionTimestamp == nil {
			matched = append(matched, p)
		}
	}
	return matched
}

func podLogs(ctx context.Context, t *testing.T, cfg *envconf.Config, pod, container string) string {
	t.Helper()
	raw, err := clientset(t, cfg).CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{Container: container}).DoRaw(ctx)
	if err != nil {
		t.Fatalf("logs %s/%s: %v", pod, container, err)
	}
	return string(raw)
}

func scaleDeployment(ctx context.Context, t *testing.T, cfg *envconf.Config, name string, replicas int32) {
	t.Helper()
	cs := clientset(t, cfg)
	scale, err := cs.AppsV1().Deployments(namespace).GetScale(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get scale %s: %v", name, err)
	}
	scale.Spec.Replicas = replicas
	if _, err := cs.AppsV1().Deployments(namespace).UpdateScale(ctx, name, scale, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("scale %s: %v", name, err)
	}
	eventually(t, 3*time.Minute, func() (bool, string) {
		d, err := cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err.Error()
		}
		return d.Status.Replicas == replicas && d.Status.ReadyReplicas == replicas, fmt.Sprintf("%s has %d/%d replicas", name, d.Status.ReadyReplicas, replicas)
	})
}

// clickhouse runs a query through the Kubernetes API server's service proxy, so
// no port-forward is needed, and returns the rows as JSON objects.
func clickhouse(ctx context.Context, t *testing.T, cfg *envconf.Config, query string) ([]map[string]any, error) {
	t.Helper()
	raw, err := clientset(t, cfg).CoreV1().RESTClient().Post().
		AbsPath("/api/v1/namespaces", namespace, "services", "http:clickhouse:http", "proxy", "").
		Param("default_format", "JSONEachRow").
		Param("database", database).
		SetHeader("X-ClickHouse-User", clickhouseUser).
		SetHeader("X-ClickHouse-Key", clickhousePassword).
		Body([]byte(query)).
		DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, bytes.TrimSpace(raw))
	}
	var rows []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		row := map[string]any{}
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("decode %q: %w", line, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// clickhouseInt returns the single integer a query selects as column n.
func clickhouseInt(ctx context.Context, t *testing.T, cfg *envconf.Config, query string) (int, error) {
	t.Helper()
	rows, err := clickhouse(ctx, t, cfg, query)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, fmt.Errorf("expected one row, got %d", len(rows))
	}
	switch v := rows[0]["n"].(type) {
	case string:
		return strconv.Atoi(v)
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("unexpected type %T for n", v)
	}
}

// expectRows waits until the query (selecting count() AS n) returns want.
func expectRows(ctx context.Context, t *testing.T, cfg *envconf.Config, query string, want int) {
	t.Helper()
	eventually(t, 2*time.Minute, func() (bool, string) {
		got, err := clickhouseInt(ctx, t, cfg, query)
		return err == nil && got == want, fmt.Sprintf("%s: got %d, want %d (err: %v)", query, got, want, err)
	})
}

func connectorTables(ctx context.Context, t *testing.T, cfg *envconf.Config) []string {
	t.Helper()
	rows, err := clickhouse(ctx, t, cfg, "SELECT name FROM system.tables WHERE database = '"+database+"' ORDER BY name")
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var names []string
	for _, r := range rows {
		names = append(names, fmt.Sprint(r["name"]))
	}
	return names
}

func columnTypes(ctx context.Context, t *testing.T, cfg *envconf.Config, table string) map[string]string {
	t.Helper()
	rows, err := clickhouse(ctx, t, cfg, fmt.Sprintf("SELECT name, type FROM system.columns WHERE database = '%s' AND table = '%s'", database, table))
	if err != nil {
		t.Fatalf("columns of %s: %v", table, err)
	}
	types := map[string]string{}
	for _, r := range rows {
		types[fmt.Sprint(r["name"])] = fmt.Sprint(r["type"])
	}
	return types
}
