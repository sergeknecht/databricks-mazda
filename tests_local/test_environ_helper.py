import pytest

from helpers.local.environ_helper import EnvironHelper

ENV_VARS = ["DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_SERVICE_NAME"]

def test_environ_helper_fail_if_not_found():

    env_helper = EnvironHelper(filename=".env.test-does-not-exist")

    with pytest.raises(IOError):
        env_vars = env_helper.get_env_vars(ENV_VARS)



def test_environ_helper_loaded_values():
    env_helper = EnvironHelper(filename=".env.test")
    env_vars = env_helper.get_env_vars(ENV_VARS)

    for var in ENV_VARS:
        assert env_vars.get(var), f"{var} is not set"
        assert env_vars[var] == f"{var}__TEST", f"{var} is not set to value: '{var}__TEST'"
