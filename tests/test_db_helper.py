from helpers.db_helper import det_connection_properties__by_key


def test_connection_properties__by_key(
    scope: str = "ACC", db_key: str = "DWH_BI1"
):
    conn_props = det_connection_properties__by_key(scope, db_key)
    assert conn_props["user"]
    assert conn_props["password"]
    assert conn_props["url"]
    assert conn_props["driver"]
    assert conn_props["fetchSize"]
    assert conn_props["scope"]
    assert conn_props["db_key"]
    assert conn_props["hostName"]
    assert conn_props["port"]
    assert conn_props["databaseName"]
    assert conn_props["db_type"]


if __name__ == "__main__":
    test_connection_properties__by_key()
    print("helpers/db_helper.py is OK")
