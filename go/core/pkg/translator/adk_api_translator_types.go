package translator

import (
	"context"

	"github.com/kagent-dev/kagent/go/api/adk"
	"github.com/kagent-dev/kagent/go/api/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"trpc.group/trpc-go/trpc-a2a-go/server"
)

type AgentOutputs struct {
	Manifest []client.Object `json:"manifest,omitempty"`

	Config    *adk.AgentConfig `json:"config,omitempty"`
	AgentCard server.AgentCard `json:"agentCard"`
}

type TranslatorPlugin interface {
	ProcessAgent(ctx context.Context, agent v1alpha2.AgentObject, outputs *AgentOutputs) error
	GetOwnedResourceTypes() []client.Object
}

// RemoteMCPServerURLRewriter optionally transforms the URL the kagent
// controller dials when discovering tools on a RemoteMCPServer.
//
// The reconciler treats the returned string as the dial target.
// Returning the input URL unchanged is the correct behavior when the
// rewriter does not apply. A nil rewriter means the controller dials
// the RemoteMCPServer spec.URL verbatim.
//
// The hook controls the URL only. TLS configuration is built from
// spec.tls independently and applied to the underlying http.Client.
// Implementations that change scheme (e.g. https:// → http:// to route
// through a destination that handles TLS elsewhere) must ensure
// spec.tls remains consistent with the new dial target. The rewriter
// currently cannot supply TLS config of its own.
type RemoteMCPServerURLRewriter interface {
	RewriteRemoteMCPServerURL(ctx context.Context, remoteMCPServer *v1alpha2.RemoteMCPServer) (string, error)
}
