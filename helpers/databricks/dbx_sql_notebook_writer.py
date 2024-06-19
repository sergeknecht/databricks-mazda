
def write_notebook_py(notebook_path, notebook_name, notebook_content: list, suffix:str = "_GENERATED_PY"):

    # prefix each line with "# MAGIC %sql\n# MAGIC "
    notebook_content = ["# MAGIC %sql\n# MAGIC " + line for line in notebook_content]
    # put a "# COMMAND ----------" between each line
    notebook_content = "\n\n# COMMAND ----------\n\n".join(notebook_content)
    # add a "# Databricks notebook source" at the beginning
    notebook_content = f"# Databricks notebook source\n{notebook_content}"

    with open(f"{notebook_path}/{notebook_name}{suffix}.py", "w") as notebook:
        notebook.write(notebook_content)
    print(f"Notebook {notebook_name}.py has been written to {notebook_path}")


def write_notebook_sql(notebook_path, notebook_name, notebook_content: list, suffix:str = "_GENERATED_SQL"):

    notebook_content = "\n\n-- COMMAND ----------\n\n".join(notebook_content)
    notebook_content = f"-- Databricks notebook source\n{notebook_content}"

    with open(f"{notebook_path}/{notebook_name}{suffix}.sql", "w") as notebook:
        notebook.write(notebook_content)
    print(f"Notebook {notebook_name}.ipynb has been written to {notebook_path}")


if __name__ == "__main__":
    notebook_path = "."
    notebook_name = "poc_sql_notebook_writer_GENERATED"
    notebook_content = ["select current_date()", "select current_date()", "select current_date()"]
    write_notebook_sql(notebook_path, notebook_name, notebook_content)
    write_notebook_py(notebook_path, notebook_name, notebook_content)
