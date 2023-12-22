# import sys
# import os

# current_dir = os.getcwd()
# target_dir = os.path.join(current_dir)

from lib.import_vars import var_test_import

assert var_test_import, "import of var_test_import failed"