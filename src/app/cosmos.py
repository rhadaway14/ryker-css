from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosHttpResponseError
from .settings import Settings

def init_cosmos(settings: Settings):
    credential = settings.COSMOS_KEY if settings.USE_COSMOS_KEY else _aad_credential()
    client = CosmosClient(settings.COSMOS_ENDPOINT, credential=credential)

    # Create DB with shared throughput (adjust RU or remove if DB already has it)
    try:
        db = client.create_database_if_not_exists(id=settings.COSMOS_DB, offer_throughput=400)
    except CosmosHttpResponseError:
        # Already exists or RU limit hit: just get the DB
        db = client.get_database_client(settings.COSMOS_DB)

    # Create container WITHOUT dedicated throughput so it uses DB-level RU
    try:
        container = db.create_container_if_not_exists(
            id=settings.COSMOS_CONTAINER,
            partition_key=PartitionKey(path=settings.COSMOS_PARTITION_KEY_PATH),
        )
    except CosmosHttpResponseError:
        container = db.get_container_client(settings.COSMOS_CONTAINER)

    return container

def _aad_credential():
    from azure.identity import DefaultAzureCredential
    return DefaultAzureCredential()
