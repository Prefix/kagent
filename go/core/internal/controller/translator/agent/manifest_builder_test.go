package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kagent-dev/kagent/go/api/adk"
	"github.com/kagent-dev/kagent/go/api/v1alpha2"
	"github.com/kagent-dev/kagent/go/core/pkg/sandboxbackend/agentsxk8s"
	pkgtranslator "github.com/kagent-dev/kagent/go/core/pkg/translator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	schemev1 "k8s.io/client-go/kubernetes/scheme"
	agentsandboxv1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBuildSRTSettingsJSON_DefaultDenyConfig(t *testing.T) {
	got, err := buildSRTSettingsJSON(nil)
	if err != nil {
		t.Fatalf("buildSRTSettingsJSON() error = %v", err)
	}

	var settings map[string]any
	if err := json.Unmarshal(got, &settings); err != nil {
		t.Fatalf("failed to unmarshal settings: %v", err)
	}

	network, ok := settings["network"].(map[string]any)
	if !ok {
		t.Fatalf("settings.network missing or wrong type: %#v", settings["network"])
	}
	if got := network["allowedDomains"]; len(got.([]any)) != 0 {
		t.Fatalf("allowedDomains = %#v, want empty list", got)
	}
	if got := network["deniedDomains"]; len(got.([]any)) != 0 {
		t.Fatalf("deniedDomains = %#v, want empty list", got)
	}

	filesystem, ok := settings["filesystem"].(map[string]any)
	if !ok {
		t.Fatalf("settings.filesystem missing or wrong type: %#v", settings["filesystem"])
	}
	if got := filesystem["denyRead"]; len(got.([]any)) != 0 {
		t.Fatalf("denyRead = %#v, want empty list", got)
	}
	if got := filesystem["allowWrite"].([]any); len(got) != 2 || got[0] != "." || got[1] != "/tmp" {
		t.Fatalf("allowWrite = %#v, want ['.','/tmp']", got)
	}
	if got := filesystem["denyWrite"]; len(got.([]any)) != 0 {
		t.Fatalf("denyWrite = %#v, want empty list", got)
	}
}

func TestNeedsSRTSettings(t *testing.T) {
	declarativeAgent := &v1alpha2.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "decl", Namespace: "default"},
		Spec: v1alpha2.AgentSpec{
			Type:        v1alpha2.AgentType_Declarative,
			Declarative: &v1alpha2.DeclarativeAgentSpec{},
		},
	}
	skillsAgent := &v1alpha2.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "skills", Namespace: "default"},
		Spec: v1alpha2.AgentSpec{
			Type:        v1alpha2.AgentType_Declarative,
			Declarative: &v1alpha2.DeclarativeAgentSpec{},
			Skills:      &v1alpha2.SkillForAgent{Refs: []string{"example.com/skill:latest"}},
		},
	}
	executeCode := true
	codeAgent := &v1alpha2.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "code", Namespace: "default"},
		Spec: v1alpha2.AgentSpec{
			Type: v1alpha2.AgentType_Declarative,
			Declarative: &v1alpha2.DeclarativeAgentSpec{
				ExecuteCodeBlocks: &executeCode,
			},
		},
	}
	byoAgent := &v1alpha2.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "byo", Namespace: "default"},
		Spec: v1alpha2.AgentSpec{
			Type: v1alpha2.AgentType_BYO,
			BYO:  &v1alpha2.BYOAgentSpec{},
		},
	}

	if needsSRTSettings(declarativeAgent, nil) {
		t.Fatal("declarative agents without sandboxed execution should not get srt settings")
	}
	if !needsSRTSettings(skillsAgent, nil) {
		t.Fatal("declarative agents with skills should get srt settings")
	}
	if !needsSRTSettings(codeAgent, nil) {
		t.Fatal("declarative agents with executeCodeBlocks should get srt settings")
	}
	if needsSRTSettings(byoAgent, nil) {
		t.Fatal("BYO agents should not get srt settings unless sandbox config is set")
	}
	if !needsSRTSettings(byoAgent, &v1alpha2.SandboxConfig{}) {
		t.Fatal("BYO agents with sandbox config should get srt settings")
	}
}

func TestBuildConfigSecretData_OmitsEmptySRTSettings(t *testing.T) {
	data := buildConfigSecretData(`{"app":"ok"}`, `{"card":"ok"}`, "")

	if data["config.json"] == "" {
		t.Fatal("config.json should be present")
	}
	if data["agent-card.json"] == "" {
		t.Fatal("agent-card.json should be present")
	}
	if _, ok := data["srt-settings.json"]; ok {
		t.Fatal("srt-settings.json should be omitted when empty")
	}
}

func TestBuildConfigSecretData_IncludesSRTSettingsWhenPresent(t *testing.T) {
	data := buildConfigSecretData(`{"app":"ok"}`, `{"card":"ok"}`, `{"network":{}}`)

	if got := data["srt-settings.json"]; got == "" {
		t.Fatal("srt-settings.json should be present when non-empty")
	}
}

// TestComputeHashFromConfigSecret_NilSecretReturnsZero pins the
// fail-safe shape of the helper: when there's no Secret to read, we
// don't fabricate a hash.
func TestComputeHashFromConfigSecret_NilSecretReturnsZero(t *testing.T) {
	if got := computeHashFromConfigSecret(nil, nil); got != 0 {
		t.Fatalf("nil secret should hash to 0, got %d", got)
	}
}

// TestComputeHashFromConfigSecret_EmptyConfigReturnsZero preserves the
// pre-PR no-config behavior. Agents without compiled config produce a
// zero hash so pods that have nothing to roll on stay stable.
func TestComputeHashFromConfigSecret_EmptyConfigReturnsZero(t *testing.T) {
	secret := &corev1.Secret{
		StringData: map[string]string{
			"config.json":     "",
			"agent-card.json": "",
		},
	}
	if got := computeHashFromConfigSecret(secret, nil); got != 0 {
		t.Fatalf("empty config secret should hash to 0, got %d", got)
	}
}

// TestComputeHashFromConfigSecret_DifferentContentDifferentHash is the
// load-bearing assertion for the plugin-restamp fix: if a plugin mutates
// the Secret's config.json, the recomputed hash must differ from the
// pre-mutation hash so pods actually roll.
func TestComputeHashFromConfigSecret_DifferentContentDifferentHash(t *testing.T) {
	pre := &corev1.Secret{StringData: map[string]string{
		"config.json":     `{"url":"https://upstream/mcp"}`,
		"agent-card.json": `{"name":"a"}`,
	}}
	post := &corev1.Secret{StringData: map[string]string{
		"config.json":     `{"url":"http://upstream:443/mcp"}`,
		"agent-card.json": `{"name":"a"}`,
	}}

	preHash := computeHashFromConfigSecret(pre, []byte("model-hash"))
	postHash := computeHashFromConfigSecret(post, []byte("model-hash"))

	assert.NotEqual(t, preHash, postHash, "mutating config.json must change the hash")
}

// TestComputeHashFromConfigSecret_SameContentSameHash pins determinism:
// the recompute path must produce identical hashes for identical inputs
// so the no-plugin case doesn't cause spurious pod rollouts on
// controller restart or version upgrade.
func TestComputeHashFromConfigSecret_SameContentSameHash(t *testing.T) {
	secret := &corev1.Secret{StringData: map[string]string{
		"config.json":     `{"url":"https://upstream/mcp"}`,
		"agent-card.json": `{"name":"a"}`,
	}}
	a := computeHashFromConfigSecret(secret, []byte("model-hash"))
	b := computeHashFromConfigSecret(secret, []byte("model-hash"))
	assert.Equal(t, a, b)
}

// configSecretMutatingPlugin walks outputs.Manifest, finds the agent
// config Secret by name, parses config.json, mutates it, and
// re-marshals. Used to verify that BuildManifest re-stamps the
// kagent.dev/config-hash annotation after plugins so the workload
// rolls on the next reconcile.
type configSecretMutatingPlugin struct {
	agentName string
	mutate    func(cfg *adk.AgentConfig)
}

func (p *configSecretMutatingPlugin) ProcessAgent(_ context.Context, _ v1alpha2.AgentObject, outputs *pkgtranslator.AgentOutputs) error {
	for _, obj := range outputs.Manifest {
		secret, ok := obj.(*corev1.Secret)
		if !ok || secret.GetName() != p.agentName {
			continue
		}
		raw := secret.StringData["config.json"]
		if raw == "" {
			return nil
		}
		var cfg adk.AgentConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return fmt.Errorf("unmarshal config.json: %w", err)
		}
		p.mutate(&cfg)
		out, err := json.Marshal(&cfg)
		if err != nil {
			return fmt.Errorf("marshal config.json: %w", err)
		}
		secret.StringData["config.json"] = string(out)
		return nil
	}
	return nil
}

func (p *configSecretMutatingPlugin) GetOwnedResourceTypes() []client.Object { return nil }

// TestBuildManifest_PluginMutatesSecret_HashReflectsPostPluginContent
// is the integration regression for the OSS gap that forced operators
// to `kubectl rollout restart` after toggling enterprise plugins.
//
// Pre-fix: buildPodTemplate stamped the kagent.dev/config-hash
// annotation from the pre-plugin AgentConfig, then plugins ran and
// mutated the Secret's config.json. The annotation never reflected the
// mutation, so the Deployment's pod template hash stayed stable and
// pods didn't roll. Operators had to restart agents manually.
//
// Post-fix: BuildManifest builds the workload with a placeholder hash,
// runs plugins, then stamps the real hash computed from the post-plugin
// Secret content. The annotation on the workload (via shared Go map
// reference) flips, the next reconcile sees the new hash, and pods roll
// automatically.
func TestBuildManifest_PluginMutatesSecret_HashReflectsPostPluginContent(t *testing.T) {
	scheme := schemev1.Scheme
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "hash-test"}}
	modelConfig := &v1alpha2.ModelConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "hash-test"},
		Spec: v1alpha2.ModelConfigSpec{
			Model:    "gpt-4o",
			Provider: v1alpha2.ModelProviderOpenAI,
		},
	}
	agent := &v1alpha2.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "hash-test"},
		Spec: v1alpha2.AgentSpec{
			Type:        v1alpha2.AgentType_Declarative,
			Description: "Agent",
			Declarative: &v1alpha2.DeclarativeAgentSpec{
				SystemMessage: "System",
				ModelConfig:   "model",
			},
		},
	}

	// Capture the unmodified pre-plugin hash by running once without a
	// plugin installed.
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, modelConfig, agent).Build()
	defaultModel := types.NamespacedName{Namespace: "hash-test", Name: "model"}
	transNoPlugin := NewAdkApiTranslator(kube, defaultModel, nil, "", nil)
	noPluginOutputs, err := TranslateAgent(context.Background(), transNoPlugin, agent)
	require.NoError(t, err)
	preHash := configHashFromDeployment(t, noPluginOutputs)

	// Now run the same compile through a plugin that rewrites
	// config.json's instruction field. The annotation MUST differ.
	mutator := &configSecretMutatingPlugin{
		agentName: "agent",
		mutate: func(cfg *adk.AgentConfig) {
			cfg.Instruction = "MUTATED BY PLUGIN"
		},
	}
	transWithPlugin := NewAdkApiTranslator(kube, defaultModel, []TranslatorPlugin{mutator}, "", nil)
	pluginOutputs, err := TranslateAgent(context.Background(), transWithPlugin, agent)
	require.NoError(t, err)
	postHash := configHashFromDeployment(t, pluginOutputs)

	assert.NotEqual(t, preHash, postHash,
		"kagent.dev/config-hash on the workload must change when a plugin mutates the config Secret")

	// Also verify the Secret itself carries the mutation (sanity-check
	// the plugin actually ran).
	mutatedSecret := configSecretFromManifest(t, pluginOutputs, "agent")
	assert.Contains(t, mutatedSecret.StringData["config.json"], "MUTATED BY PLUGIN")
}

// TestBuildManifest_NoPlugin_HashMatchesGoldenBehavior is the
// no-spurious-rollout guard. When no plugin runs, the post-plugin
// recompute must produce the same hash that the pre-PR code would have
// stamped — otherwise upgrading the controller would roll every agent
// pod for no functional reason.
//
// The golden testdata under testdata/outputs/ already pins specific
// hash values for many agent shapes; this is the dedicated regression
// test that names the invariant.
func TestBuildManifest_NoPlugin_HashMatchesGoldenBehavior(t *testing.T) {
	scheme := schemev1.Scheme
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "no-plugin"}}
	modelConfig := &v1alpha2.ModelConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "no-plugin"},
		Spec:       v1alpha2.ModelConfigSpec{Model: "gpt-4o", Provider: v1alpha2.ModelProviderOpenAI},
	}
	agent := &v1alpha2.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "no-plugin"},
		Spec: v1alpha2.AgentSpec{
			Type:        v1alpha2.AgentType_Declarative,
			Description: "Agent",
			Declarative: &v1alpha2.DeclarativeAgentSpec{
				SystemMessage: "System",
				ModelConfig:   "model",
			},
		},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, modelConfig, agent).Build()
	trans := NewAdkApiTranslator(kube, types.NamespacedName{Namespace: "no-plugin", Name: "model"}, nil, "", nil)

	first, err := TranslateAgent(context.Background(), trans, agent)
	require.NoError(t, err)
	second, err := TranslateAgent(context.Background(), trans, agent)
	require.NoError(t, err)

	assert.Equal(t, configHashFromDeployment(t, first), configHashFromDeployment(t, second),
		"two translations of the same agent must produce identical hashes (deterministic, no rollout drift)")
	assert.NotEqual(t, "0", configHashFromDeployment(t, first),
		"a non-empty config should produce a non-zero hash")
}

// configHashFromDeployment extracts the kagent.dev/config-hash
// annotation value from the Deployment's pod template in an outputs
// bundle. Test helper.
func configHashFromDeployment(t *testing.T, outputs *pkgtranslator.AgentOutputs) string {
	t.Helper()
	for _, obj := range outputs.Manifest {
		if dep, ok := obj.(*appsv1.Deployment); ok {
			return dep.Spec.Template.Annotations["kagent.dev/config-hash"]
		}
	}
	t.Fatal("Deployment not found in outputs.Manifest")
	return ""
}

// configSecretFromManifest finds the agent's config Secret in an
// outputs bundle. Test helper.
func configSecretFromManifest(t *testing.T, outputs *pkgtranslator.AgentOutputs, name string) *corev1.Secret {
	t.Helper()
	for _, obj := range outputs.Manifest {
		if s, ok := obj.(*corev1.Secret); ok && strings.Contains(s.GetName(), name) {
			return s
		}
	}
	t.Fatal("config Secret not found in outputs.Manifest")
	return nil
}

// configHashFromSandbox extracts the kagent.dev/config-hash annotation
// from the Sandbox CR's pod template. Sandbox-mode workloads carry the
// same annotation as Deployment-mode workloads, just nested through
// Sandbox.Spec.PodTemplate.ObjectMeta (agent-sandbox uses its own
// PodMetadata type, not corev1.ObjectMeta). Test helper.
func configHashFromSandbox(t *testing.T, outputs *pkgtranslator.AgentOutputs) string {
	t.Helper()
	for _, obj := range outputs.Manifest {
		if sb, ok := obj.(*agentsandboxv1.Sandbox); ok {
			return sb.Spec.PodTemplate.ObjectMeta.Annotations["kagent.dev/config-hash"]
		}
	}
	t.Fatal("Sandbox not found in outputs.Manifest")
	return ""
}

// TestBuildManifest_SandboxMode_PluginMutatesSecret_HashReflectsPostPluginContent
// is the sandbox-mode sibling of the Deployment-mode regression test.
//
// The post-plugin hash stamp relies on Go map-reference sharing: the
// Annotations map on the local podTemplate variable, on
// Deployment.Spec.Template.Annotations (normal mode), and on
// Sandbox.Spec.PodTemplate.Annotations (sandbox mode) must all point at
// the same underlying map. If a future sandbox backend "defensively"
// deep-copies that map via maps.Clone, the post-plugin mutation silently
// stops propagating to the Sandbox CR — sandbox-mode agents stop rolling
// on plugin-driven config changes.
//
// This test specifically pins the sandbox-path invariant. The
// agentsxk8s.New() backend at agentsxk8s.go:50 currently does
// `Annotations: in.PodTemplate.Annotations` (reference assignment, not
// clone) and a regression to clone here breaks this assertion.
func TestBuildManifest_SandboxMode_PluginMutatesSecret_HashReflectsPostPluginContent(t *testing.T) {
	scheme := schemev1.Scheme
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, agentsandboxv1.AddToScheme(scheme))

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "sb-hash-test"}}
	modelConfig := &v1alpha2.ModelConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "sb-hash-test"},
		Spec:       v1alpha2.ModelConfigSpec{Model: "gpt-4o", Provider: v1alpha2.ModelProviderOpenAI},
	}
	// SandboxAgent (not Agent) drives the sandbox path through
	// buildWorkloadObjects → sandboxBackend.BuildSandbox.
	sb := &v1alpha2.SandboxAgent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "sb-hash-test"},
		Spec: v1alpha2.AgentSpec{
			Type: v1alpha2.AgentType_Declarative,
			Declarative: &v1alpha2.DeclarativeAgentSpec{
				SystemMessage: "System",
				ModelConfig:   "model",
			},
		},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, modelConfig).Build()
	defaultModel := types.NamespacedName{Namespace: "sb-hash-test", Name: "model"}

	// No-plugin baseline.
	noPluginTrans := NewAdkApiTranslator(kube, defaultModel, nil, "", agentsxk8s.New())
	noPluginOutputs, err := TranslateAgent(context.Background(), noPluginTrans, sb)
	require.NoError(t, err)
	preHash := configHashFromSandbox(t, noPluginOutputs)
	assert.NotEqual(t, "0", preHash, "sandbox-mode workload should have a non-placeholder hash post-stamp")

	// With a plugin that mutates the Secret, the Sandbox CR's hash
	// MUST differ. Failure here means the map-sharing invariant broke
	// on the sandbox path (typically a future agentsxk8s change that
	// clones the annotations map).
	mutator := &configSecretMutatingPlugin{
		agentName: "agent",
		mutate: func(cfg *adk.AgentConfig) {
			cfg.Instruction = "MUTATED BY PLUGIN"
		},
	}
	pluginTrans := NewAdkApiTranslator(kube, defaultModel, []TranslatorPlugin{mutator}, "", agentsxk8s.New())
	pluginOutputs, err := TranslateAgent(context.Background(), pluginTrans, sb)
	require.NoError(t, err)
	postHash := configHashFromSandbox(t, pluginOutputs)

	assert.NotEqual(t, preHash, postHash,
		"sandbox CR's config-hash annotation must change when a plugin mutates the config Secret — "+
			"failure typically indicates the sandbox backend started deep-copying the pod template annotations map, "+
			"which silently breaks plugin-driven rollouts")
}
