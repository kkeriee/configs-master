from .provider_github import GithubCollector
collector_registry = {
    'github_public_lists': GithubCollector()
}
