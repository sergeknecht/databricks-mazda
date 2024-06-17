import os

from dotenv import find_dotenv, load_dotenv


class EnvironHelper:
    def __init__(self, filename: str = ".env"):
        self.initialized = False
        self.env_vars = {}
        self.filename = filename

    def load_env(self):
        # print(f"Loading environment variables from {self.filename}")
        path = find_dotenv(filename=self.filename, raise_error_if_not_found=True)
        # assert path, f"Path from {self.filename} not found: {path}"
        # print(f"Loading environment variables from {self.filename}: {path}")
        path_found = load_dotenv(dotenv_path=path)
        assert path_found, f"Path from {self.filename} not found: {path_found}"
        # print(f"Environment variables loaded: {path_found}")


    def get_env_vars(self, var_names: list[str]) -> dict[str, str]:
        if not self.initialized:
            self.load_env()
            self.initialized = True

        for var_name in var_names:
            var_value = os.getenv(var_name)
            if var_value is not None:
                self.env_vars[var_name] = var_value

        return self.env_vars
