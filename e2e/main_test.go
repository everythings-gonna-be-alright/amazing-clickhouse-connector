//go:build e2e

package e2e

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/support/kind"
	"sigs.k8s.io/e2e-framework/third_party/helm"
)

const (
	namespace   = "clickhouse-connector-e2e"
	releaseName = "clickhouse-connector"
	image       = "clickhouse-connector:e2e"
	chartPath   = "../helm/clickhouse-connector"

	serverDeployment   = releaseName + "-server"
	consumerDeployment = releaseName + "-consumer"
	connectorURL       = "http://" + releaseName + ":6000"
	valkeyService      = releaseName + "-valkey"
	releaseSecret      = releaseName + "-secrets"
	database           = "clickhouse_connector"
	clickhouseUser     = "connector"
	queueName          = "rmq::queue::[clickhouse_connector]::ready"
)

var (
	testenv     env.Environment
	clusterName = envOr("E2E_CLUSTER_NAME", "clickhouse-connector-test")
	keepCluster = os.Getenv("E2E_KEEP_CLUSTER") == "1"

	// Credentials are random per run and only ever live in the throwaway cluster.
	authToken          = rand.Text()
	clickhousePassword = rand.Text()
)

func TestMain(m *testing.M) {
	// kind writes the new cluster into $KUBECONFIG and switches the current
	// context, so keep it away from the user's real kubeconfig.
	kubeconfigDir, err := os.MkdirTemp("", "clickhouse-connector-e2e-")
	if err != nil {
		log.Fatalf("create kubeconfig dir: %v", err)
	}
	if err := os.Setenv("KUBECONFIG", filepath.Join(kubeconfigDir, "kubeconfig")); err != nil {
		log.Fatalf("set KUBECONFIG: %v", err)
	}

	testenv = env.New()

	testenv.Setup(
		buildImage,
		envfuncs.CreateCluster(kind.NewProvider(), clusterName),
		envfuncs.CreateNamespace(namespace),
		envfuncs.LoadImageToCluster(clusterName, image),
		installClickHouse,
		installChart,
	)

	var finish []env.Func
	if dir := os.Getenv("E2E_LOGS_DIR"); dir != "" {
		finish = append(finish, envfuncs.ExportClusterLogs(clusterName, dir))
	}
	if !keepCluster {
		finish = append(finish, envfuncs.DestroyCluster(clusterName))
	}
	testenv.Finish(finish...)

	code := testenv.Run(m)
	if err := os.RemoveAll(kubeconfigDir); err != nil {
		log.Printf("remove kubeconfig dir: %v", err)
	}
	os.Exit(code)
}

func buildImage(ctx context.Context, _ *envconf.Config) (context.Context, error) {
	cmd := exec.CommandContext(ctx, "docker", "build", "-t", image, "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		return ctx, fmt.Errorf("docker build: %w\n%s", err, out)
	}
	return ctx, nil
}

func installClickHouse(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	raw, err := os.ReadFile("testdata/clickhouse.yaml")
	if err != nil {
		return ctx, err
	}
	manifest := strings.ReplaceAll(string(raw), "__CLICKHOUSE_PASSWORD__", clickhousePassword)

	r := cfg.Client().Resources(namespace)
	if err := decoder.DecodeEach(ctx, strings.NewReader(manifest), decoder.CreateHandler(r), decoder.MutateNamespace(namespace)); err != nil {
		return ctx, fmt.Errorf("apply clickhouse manifest: %w", err)
	}

	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "clickhouse", Namespace: namespace}}
	err = wait.For(conditions.New(r).DeploymentAvailable(deployment.Name, namespace),
		wait.WithTimeout(10*time.Minute), wait.WithInterval(2*time.Second), wait.WithContext(ctx))
	if err != nil {
		return ctx, fmt.Errorf("clickhouse did not become available: %w", err)
	}
	return ctx, nil
}

func installChart(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	err := helm.New(cfg.KubeconfigFile()).RunUpgrade(
		helm.WithName(releaseName),
		helm.WithNamespace(namespace),
		helm.WithChart(chartPath),
		helm.WithArgs(
			"--install",
			"--values", "testdata/values.yaml",
			"--set-string", "config.authKey="+authToken,
			"--set-string", "config.clickhouse.password="+clickhousePassword,
		),
		helm.WithWait(),
		helm.WithTimeout("10m"),
		helm.WithContext(ctx),
	)
	if err != nil {
		return ctx, fmt.Errorf("helm install: %w", err)
	}
	return ctx, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
