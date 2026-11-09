"""Optional Dagster GitHub resource, based on the original production integration."""

from dagster import ConfigurableResource
from github import Github
from pydantic import Field
from urllib3.util.retry import Retry


class BoundedRetry(Retry):
    def get_retry_after(self, response):
        delay = super().get_retry_after(response)
        return min(delay, 120) if delay is not None else None


class GitHubResource(ConfigurableResource):
    api_token: str = Field(repr=False)
    timeout_seconds: int = 30
    max_retries: int = 5

    def get_client(self) -> Github:
        retry = BoundedRetry(
            total=self.max_retries,
            backoff_factor=2,
            status_forcelist=(403, 429, 500, 502, 503, 504),
            allowed_methods=None,
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        return Github(
            self.api_token, timeout=self.timeout_seconds, retry=retry, per_page=100
        )
