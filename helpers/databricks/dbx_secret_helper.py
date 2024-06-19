def get_secret(scope: str, key: str):
    scope=scope.upper()
    key=key.upper()
    key=f"{scope}__{key}
    secret = dbutils.secrets.get(
        scope=scope.upper(), key=key
    )

    assert secret, f"secret '{key}' not found in scope: {scope}"