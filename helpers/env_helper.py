# Importing required libraries
import os
from typing import List

from dotenv import load_dotenv


def load_env_as_json(dev_mode='ACC', env_filename='.env') -> dict:
    # Load .env file
    load_dotenv(dotenv_path=env_filename)

    # Get secrets from .env file and save them to the scope
    env_dict = {}
    for key, value in os.environ.items():
        split_key = key.split('__')
        if len(split_key) == 3:
            k_mode = split_key[0]
            k_key = split_key[1]
            k_subkey = split_key[2]
            if k_mode not in env_dict:
                env_dict[k_mode] = {}
            if k_key not in env_dict[k_mode]:
                env_dict[k_mode][k_key] = {}
            env_dict[k_mode][k_key][k_subkey] = value

    # Return the secrets
    return env_dict[dev_mode]


def load_env_as_list(dev_mode='ACC', env_filename='.env') -> List:
    # Load .env file
    load_dotenv(dotenv_path=env_filename)

    # Get secrets from .env file and save them to the scope
    env = []
    for key, value in os.environ.items():
        split_key = key.split('__')
        if len(split_key) == 3:
            k_mode = split_key[0]
            k_key = split_key[1]
            k_subkey = split_key[2]
            if k_mode == dev_mode:
                env.append((f'{k_key}__{k_subkey}', value))

    # Return the secrets
    return env


if __name__ == '__main__':
    env_dict = load_env_as_json()

    import pprint as pp

    pp.pprint(env_dict)
