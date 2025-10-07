from google.cloud import secretmanager

from finance_pipeline.config import PROJECT_ID


def get_secret(secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode(
        "UTF-8"
    )
