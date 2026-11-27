# Security

## Reporting vulnerabilities

Please report vulnerabilities privately rather than opening a public issue containing sensitive details. Use GitHub's private vulnerability reporting feature if available, or contact the repository maintainer to arrange a private report.

## Deployment

Keep configuration secrets and persistent crypto stores private. Restrict access to the deployment's `generated/` and `worker-state/` directories. Preserve both when upgrading or restoring the service.

See the [installation guide](deployment/mautrix-whatsapp-call/INSTALLATION.md) and [operations guide](deployment/mautrix-whatsapp-call/OPERATIONS.md) for configuration and credential management.
