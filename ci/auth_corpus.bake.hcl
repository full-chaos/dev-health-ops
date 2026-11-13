# CHAOS-13520: the build cache of the six images the auth corpus runs (ci/auth_corpus_external.sh), merged by docker buildx bake
# with compose.yml and ci/auth_corpus_external.override.yml (same target names). Each image reads its own gha scope; only a run
# that sets AUTH_CORPUS_CACHE_WRITE=1 writes it (go-auth-service.yml: auth-corpus-image-cache, a push to main of this repository).
# The pull request legs read only. internal/cigates/workflows/auth_corpus_build_cache_test.go holds this file and the workflow.
variable "AUTH_CORPUS_CACHE_WRITE" {
  default = ""
}

group "authcorpus" {
  targets = ["auth-service", "auth-migrate", "go-api", "auth-keys-init", "providerstub", "smtpsink"]
}

target "auth-service" {
  cache-from = ["type=gha,scope=authcorpus-auth-service"]
  cache-to   = AUTH_CORPUS_CACHE_WRITE == "1" ? ["type=gha,scope=authcorpus-auth-service,mode=max"] : []
}

target "auth-migrate" {
  cache-from = ["type=gha,scope=authcorpus-auth-migrate"]
  cache-to   = AUTH_CORPUS_CACHE_WRITE == "1" ? ["type=gha,scope=authcorpus-auth-migrate,mode=max"] : []
}

target "go-api" {
  cache-from = ["type=gha,scope=authcorpus-go-api"]
  cache-to   = AUTH_CORPUS_CACHE_WRITE == "1" ? ["type=gha,scope=authcorpus-go-api,mode=max"] : []
}

target "auth-keys-init" {
  cache-from = ["type=gha,scope=authcorpus-auth-keys-init"]
  cache-to   = AUTH_CORPUS_CACHE_WRITE == "1" ? ["type=gha,scope=authcorpus-auth-keys-init,mode=max"] : []
}

target "providerstub" {
  cache-from = ["type=gha,scope=authcorpus-providerstub"]
  cache-to   = AUTH_CORPUS_CACHE_WRITE == "1" ? ["type=gha,scope=authcorpus-providerstub,mode=max"] : []
}

target "smtpsink" {
  cache-from = ["type=gha,scope=authcorpus-smtpsink"]
  cache-to   = AUTH_CORPUS_CACHE_WRITE == "1" ? ["type=gha,scope=authcorpus-smtpsink,mode=max"] : []
}
