# Security Documentation

## Overview
This document outlines the security measures implemented in the FEC Query application for internal use.

## Security Measures Implemented

### 1. Secrets Management
- **No API keys in code**: All API keys are stored in environment variables or database (UI-configured)
- **`.env` files excluded**: `.env` files are properly excluded from version control via `.gitignore`
- **Example file**: `env.example` contains only placeholder values, no real secrets

### 2. Input Validation
- **Query parameter validation**: All endpoints validate input parameters using Pydantic and FastAPI validators
- **Type checking**: Parameters are validated for correct types (int, str, date formats)
- **Range validation**: Numeric values have min/max constraints (e.g., cycle: 2000-2100, limits: 1-10000)
- **String sanitization**: User input is sanitized to prevent SQL injection (though SQLAlchemy handles parameterization)

### 3. SQL Injection Prevention
- **Parameterized queries**: All SQL queries use parameterized statements with `:parameter` syntax
- **SQLAlchemy ORM**: Most queries use SQLAlchemy ORM which automatically parameterizes queries
- **Raw SQL review**: All `text()` SQL queries have been reviewed and use proper parameterization

### 4. Rate Limiting
- **slowapi integration**: Rate limiting middleware implemented using `slowapi`
- **Different limits**: Different rate limits for different operation types:
  - Read operations: 100/minute
  - Write operations: 30/minute
  - Expensive operations: 10/minute
  - Bulk operations: 5/minute

### 5. Resource Limits
- **Concurrent job limits**: Maximum 3 concurrent bulk import operations (configurable via `MAX_CONCURRENT_JOBS`)
- **Request size limits**: Maximum 50MB request size (configurable via `MAX_REQUEST_SIZE_MB`)
- **File size limits**: Maximum 1000MB file size (configurable via `MAX_FILE_SIZE_MB`)

### 6. CORS Configuration
- **Restricted origins**: CORS is restricted to specific origins (configurable via `CORS_ORIGINS`)
- **Explicit methods**: Only specific HTTP methods allowed: GET, POST, PUT, DELETE, OPTIONS
- **Explicit headers**: Only specific headers allowed: Content-Type, Authorization, Accept

### 7. Security Headers
- **X-Content-Type-Options**: Prevents MIME type sniffing
- **X-Frame-Options**: Prevents clickjacking (set to DENY)
- **X-XSS-Protection**: Enables XSS filtering
- **Referrer-Policy**: Controls referrer information
- **HSTS**: Strict Transport Security (only enabled when `USE_HTTPS=true`)

### 8. Docker Security
- **Non-root user**: Application runs as non-root user (`appuser`)
- **Minimal base image**: Uses `python:3.11-slim` for smaller attack surface
- **Proper permissions**: File permissions set correctly for non-root user

### 9. Security Logging
- **Security events**: All security-related events are logged:
  - Rate limit violations
  - Resource limit exceeded
  - Admin operations (DELETE, bulk operations)
- **Event details**: Logs include client IP, path, method, and operation details

## Environment Variables

### Security-Related Configuration
- `CORS_ORIGINS`: Comma-separated list of allowed CORS origins
- `USE_HTTPS`: Set to "true" to enable HSTS header
- `MAX_CONCURRENT_JOBS`: Maximum concurrent bulk import jobs (default: 3)
- `MAX_REQUEST_SIZE_MB`: Maximum request size in MB (default: 50)
- `MAX_FILE_SIZE_MB`: Maximum file size in MB (default: 1000)

## Dependency Security

### Recommended Actions
1. **Regular audits**: Run `pip-audit` and `npm audit` regularly to check for vulnerabilities
2. **Update dependencies**: Keep dependencies updated to latest secure versions
3. **Monitor advisories**: Subscribe to security advisories for used packages

### Running Security Audits
```bash
# Python dependencies
cd backend
pip-audit

# Node dependencies
cd frontend
npm audit
```

## Best Practices

1. **Never commit secrets**: Always use environment variables or secure storage
2. **Regular updates**: Keep dependencies updated
3. **Monitor logs**: Regularly review security event logs
4. **Access control**: Since this is internal-only, ensure network-level access controls
5. **Backup security**: Ensure database backups are stored securely

## Internal Use Considerations

Since this application is for internal use only:
- **No authentication required**: Endpoints are accessible to anyone on the internal network
- **Network security**: Rely on network-level security (firewalls, VPNs, etc.)
- **Access control**: Implement at infrastructure level (reverse proxy, network policies)

## Incident Response

If a security issue is discovered:
1. Immediately assess the scope and impact
2. Take affected systems offline if necessary
3. Review security logs for suspicious activity
4. Update security measures as needed
5. Document the incident and remediation steps

