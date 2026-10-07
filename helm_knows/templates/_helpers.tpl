{{/*
Name of the Redis Deployment/Service, which is also the host the app reads from
REDIS_ENV. Prod uses the default "redis"; the dev release (same namespace) sets
redis.name so it gets its own Redis and Celery queue instead of sharing prod's.
*/}}
{{- define "nickknows.redisName" -}}
{{- (.Values.redis | default dict).name | default "redis" -}}
{{- end -}}
