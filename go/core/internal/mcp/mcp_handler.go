package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	a2aclientv2 "github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/kagent-dev/kagent/go/api/v1alpha2"
	"github.com/kagent-dev/kagent/go/core/internal/a2a"
	"github.com/kagent-dev/kagent/go/core/internal/version"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// MCPHandler handles MCP requests and bridges them to A2A endpoints
type MCPHandler struct {
	kubeClient    client.Client
	a2aBaseURL    string
	a2aTimeout    time.Duration
	authenticator auth.AuthProvider
	httpHandler   *mcpsdk.StreamableHTTPHandler
	server        *mcpsdk.Server
	a2aClients    sync.Map
}

// Input types for MCP tools
type ListAgentsInput struct{}

type ListAgentsOutput struct {
	Agents []AgentSummary `json:"agents"`
}

type AgentSummary struct {
	Ref         string `json:"ref"`
	Description string `json:"description,omitempty"`
}

type InvokeAgentInput struct {
	Agent     string `json:"agent" jsonschema:"Agent reference in format namespace/name"`
	Task      string `json:"task" jsonschema:"Task to run"`
	ContextID string `json:"context_id,omitempty" jsonschema:"Optional A2A context ID to continue a conversation"`
}

type InvokeAgentOutput struct {
	Agent     string `json:"agent"`
	Text      string `json:"text"`
	ContextID string `json:"context_id,omitempty"`
}

// defaultA2ATimeout is the fallback timeout for A2A client calls and should match
// the configured default streaming timeout.
const defaultA2ATimeout = 10 * time.Minute

// NewMCPHandler creates a new MCP handler
// Wraps the StreamableHTTPHandler and adds A2A bridging and context management.
func NewMCPHandler(kubeClient client.Client, a2aBaseURL string, authenticator auth.AuthProvider, a2aTimeout time.Duration) (*MCPHandler, error) {
	if a2aTimeout <= 0 {
		a2aTimeout = defaultA2ATimeout
	}
	handler := &MCPHandler{
		kubeClient:    kubeClient,
		a2aBaseURL:    a2aBaseURL,
		a2aTimeout:    a2aTimeout,
		authenticator: authenticator,
	}

	// Create MCP server
	impl := &mcpsdk.Implementation{
		Name:    "kagent-agents",
		Version: version.Version,
	}
	server := mcpsdk.NewServer(impl, nil)
	handler.server = server

	// Add list_agents tool
	mcpsdk.AddTool[ListAgentsInput, ListAgentsOutput](
		server,
		&mcpsdk.Tool{
			Name:        "list_agents",
			Description: "List invokable kagent agents (accepted + deploymentReady)",
		},
		handler.handleListAgents,
	)

	// Add invoke_agent tool
	mcpsdk.AddTool[InvokeAgentInput, InvokeAgentOutput](
		server,
		&mcpsdk.Tool{
			Name:        "invoke_agent",
			Description: "Invoke a kagent agent via A2A",
		},
		handler.handleInvokeAgent,
	)

	// Create HTTP handler
	handler.httpHandler = mcpsdk.NewStreamableHTTPHandler(
		func(*http.Request) *mcpsdk.Server {
			return server
		},
		nil,
	)

	return handler, nil
}

// handleListAgents handles the list_agents MCP tool
func (h *MCPHandler) handleListAgents(ctx context.Context, req *mcpsdk.CallToolRequest, input ListAgentsInput) (*mcpsdk.CallToolResult, ListAgentsOutput, error) {
	log := ctrllog.FromContext(ctx).WithName("mcp-handler").WithValues("tool", "list_agents")

	agentList := &v1alpha2.AgentList{}
	if err := h.kubeClient.List(ctx, agentList); err != nil {
		return &mcpsdk.CallToolResult{
			Content: []mcpsdk.Content{
				&mcpsdk.TextContent{Text: fmt.Sprintf("Failed to list agents: %v", err)},
			},
			IsError: true,
		}, ListAgentsOutput{}, nil
	}

	agents := make([]AgentSummary, 0)
	for _, agent := range agentList.Items {
		// Check if agent is accepted and deployment ready
		deploymentReady := false
		accepted := false
		for _, condition := range agent.Status.Conditions {
			if condition.Type == "Ready" && condition.Reason == "DeploymentReady" && condition.Status == "True" {
				deploymentReady = true
			}
			if condition.Type == "Accepted" && condition.Status == "True" {
				accepted = true
			}
		}

		if !accepted || !deploymentReady {
			continue
		}

		ref := agent.Namespace + "/" + agent.Name
		description := agent.Spec.Description
		agents = append(agents, AgentSummary{
			Ref:         ref,
			Description: description,
		})
	}

	log.Info("Listed agents", "count", len(agents))

	output := ListAgentsOutput{Agents: agents}

	var fallbackText strings.Builder
	if len(agents) == 0 {
		fallbackText.WriteString("No invokable agents found.")
	} else {
		for i, agent := range agents {
			if i > 0 {
				fallbackText.WriteByte('\n')
			}
			fallbackText.WriteString(agent.Ref)
			if agent.Description != "" {
				fallbackText.WriteString(" - ")
				fallbackText.WriteString(agent.Description)
			}
		}
	}

	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{
			&mcpsdk.TextContent{Text: fallbackText.String()},
		},
	}, output, nil
}

// handleInvokeAgent handles the invoke_agent MCP tool
func (h *MCPHandler) handleInvokeAgent(ctx context.Context, req *mcpsdk.CallToolRequest, input InvokeAgentInput) (*mcpsdk.CallToolResult, InvokeAgentOutput, error) {
	log := ctrllog.FromContext(ctx).WithName("mcp-handler").WithValues("tool", "invoke_agent")

	// Parse agent reference (namespace/name or just name)
	agentNS, agentName, ok := strings.Cut(input.Agent, "/")
	if !ok {
		return &mcpsdk.CallToolResult{
			Content: []mcpsdk.Content{
				&mcpsdk.TextContent{Text: "agent must be in format 'namespace/name'"},
			},
			IsError: true,
		}, InvokeAgentOutput{}, nil
	}
	agentRef := agentNS + "/" + agentName
	agentNns := types.NamespacedName{Namespace: agentNS, Name: agentName}

	// Get or create cached A2A client for this agent
	a2aURL := fmt.Sprintf("%s/%s/", h.a2aBaseURL, agentRef)
	var a2aClient *a2aclientv2.Client

	if cached, ok := h.a2aClients.Load(agentRef); ok {
		if client, ok := cached.(*a2aclientv2.Client); ok {
			a2aClient = client
		}
	}

	// Create new client if not cached
	if a2aClient == nil {
		httpClient := &http.Client{Timeout: h.a2aTimeout}
		resolver := agentcard.NewResolver(httpClient)
		card, err := resolver.Resolve(ctx, a2aURL)
		if err != nil {
			log.Error(err, "Failed to resolve A2A card", "agent", agentRef)
			return &mcpsdk.CallToolResult{
				Content: []mcpsdk.Content{
					&mcpsdk.TextContent{Text: fmt.Sprintf("Failed to resolve A2A card: %v", err)},
				},
				IsError: true,
			}, InvokeAgentOutput{}, nil
		}
		newClient, err := a2aclientv2.NewFromCard(
			ctx,
			card,
			a2aclientv2.WithJSONRPCTransport(httpClient),
			a2aclientv2.WithCallInterceptors(
				a2a.NewUpstreamAuthInterceptor(h.authenticator, agentNns),
				a2a.NewStaticHeadersInterceptor(map[string]string{"A2A-Version": string(a2atype.Version)}),
			),
		)
		if err != nil {
			log.Error(err, "Failed to create A2A client", "agent", agentRef)
			return &mcpsdk.CallToolResult{
				Content: []mcpsdk.Content{
					&mcpsdk.TextContent{Text: fmt.Sprintf("Failed to create A2A client: %v", err)},
				},
				IsError: true,
			}, InvokeAgentOutput{}, nil
		}

		// Cache the client
		h.a2aClients.Store(agentRef, newClient)
		a2aClient = newClient
	}

	message := a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart(input.Task))
	message.ContextID = input.ContextID
	result, err := a2aClient.SendMessage(ctx, &a2atype.SendMessageRequest{Message: message})
	if err != nil {
		log.Error(err, "Failed to send A2A message", "agent", agentRef)
		return &mcpsdk.CallToolResult{
			Content: []mcpsdk.Content{
				&mcpsdk.TextContent{Text: fmt.Sprintf("Failed to send A2A message: %v", err)},
			},
			IsError: true,
		}, InvokeAgentOutput{}, nil
	}

	// Extract response text and context ID
	var responseText, newContextID string
	switch a2aResult := result.(type) {
	case *a2atype.Message:
		responseText = a2a.ExtractText(a2aResult)
		newContextID = a2aResult.ContextID
	case *a2atype.Task:
		newContextID = a2aResult.ContextID
		if a2aResult.Status.Message != nil {
			responseText = a2a.ExtractText(a2aResult.Status.Message)
		}
		for _, artifact := range a2aResult.Artifacts {
			responseText += a2a.ExtractText(&a2atype.Message{Parts: artifact.Parts})
		}
	}

	if responseText == "" {
		raw, err := json.Marshal(result)
		if err != nil {
			return &mcpsdk.CallToolResult{
				Content: []mcpsdk.Content{
					&mcpsdk.TextContent{Text: fmt.Sprintf("Failed to marshal result: %v", err)},
				},
				IsError: true,
			}, InvokeAgentOutput{}, nil
		}
		responseText = string(raw)
	}

	log.Info("Invoked agent", "agent", agentRef, "hasContextID", newContextID != "")

	// Return context_id in response so client can store it for stateless operation
	output := InvokeAgentOutput{
		Agent: agentRef,
		Text:  responseText,
	}
	if newContextID != "" {
		output.ContextID = newContextID
	}

	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{
			&mcpsdk.TextContent{Text: responseText},
		},
	}, output, nil
}

// ServeHTTP implements http.Handler interface
func (h *MCPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The MCP HTTP handler handles all the routing internally
	h.httpHandler.ServeHTTP(w, r)
}

// Shutdown gracefully shuts down the MCP handler
func (h *MCPHandler) Shutdown(ctx context.Context) error {
	// The new SDK doesn't have an explicit Shutdown method on StreamableHTTPHandler
	// The server will be shut down when the context is cancelled
	return nil
}
