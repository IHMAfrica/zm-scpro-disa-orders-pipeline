This directory contains Kubernetes manifests intended to be consumed by Rancher Fleet.

- flink-sessionjob.yaml: Defines a FlinkSessionJob that submits the job to an existing Flink session cluster named `session-cluster` managed by the Flink Kubernetes Operator.
- Note: The session cluster itself is assumed to be managed separately. If you need a session cluster definition, coordinate with your platform team to provide a FlinkDeployment named `session-cluster`.

Image reference:
- By default, the manifest points to the `:latest` tag in GitHub Releases for convenience. In production, Fleet can be configured to pin to immutable tags (e.g., Git SHA) via overlays or automated substitutions.
