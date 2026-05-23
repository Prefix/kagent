package openclaw

const (
	// bootstrapSecretProviderID is the secrets.providers key written into openclaw.json.
	bootstrapSecretProviderID = "kagent"

	// DefaultInferenceBaseURL is the Model provider baseUrl when ModelConfig does not set an explicit upstream (OpenShell).
	DefaultInferenceBaseURL = "https://inference.local/v1"

	// SubstrateBootstrapDefaultBaseURL is passed to BuildBootstrapJSON for Substrate harnesses.
	// When ModelConfig has no explicit provider URL, the models section is omitted entirely so
	// OpenClaw is not given a partial providers.* block (baseUrl is required when present).
	SubstrateBootstrapDefaultBaseURL = ""
)
