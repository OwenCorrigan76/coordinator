/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package steps

import (
	"context"
	"errors"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"

	"github.com/llm-d/coordinator/pkg/gateway"
	"github.com/llm-d/coordinator/pkg/pipeline"
)

const GatewayProxyStepName = "gateway-proxy"

func init() {
	pipeline.Register(GatewayProxyStepName, NewGatewayProxyStep)
}

// GatewayProxyStep is a simple pass-through step that forwards the request
// to the gateway without modification. Useful for multimodal endpoints that
// don't need the full disaggregated pipeline (encode/prefill/decode).
type GatewayProxyStep struct {
	gwClient   *gateway.Client
	targetPath string
}

// NewGatewayProxyStep creates a new gateway proxy step.
func NewGatewayProxyStep(gwClient *gateway.Client, params map[string]any) (pipeline.Step, error) {
	if gwClient == nil {
		return nil, errors.New("gateway-proxy: gateway client is required")
	}

	step := &GatewayProxyStep{
		gwClient: gwClient,
	}

	// Optional: override the target path
	if targetPath, ok := params["target_path"].(string); ok {
		step.targetPath = targetPath
	}

	return step, nil
}

func (s *GatewayProxyStep) Name() string {
	return GatewayProxyStepName
}

func (s *GatewayProxyStep) Execute(ctx context.Context, reqCtx *pipeline.RequestContext) error {
	logger := log.FromContext(ctx).WithName(GatewayProxyStepName)

	// Use target path if specified, otherwise use the original request path
	targetPath := reqCtx.OriginalPath
	if s.targetPath != "" {
		targetPath = s.targetPath
		reqCtx.OriginalPath = s.targetPath
	}

	logger.V(logutil.DEFAULT).Info("proxying request", "path", targetPath, "stream", reqCtx.Stream)

	// Create proxy request using the same pattern as decode step
	proxyReq, err := newDecodeProxyRequest(ctx, logger, GatewayProxyStepName, reqCtx, s.gwClient, reqCtx.Body, nil)
	if err != nil {
		return err
	}

	// Create reverse proxy and stream response to client
	proxy := newDecodeProxy(logger, s.gwClient.Transport(), nil)
	proxy.ServeHTTP(reqCtx.ResponseWriter, proxyReq)

	return nil
}
