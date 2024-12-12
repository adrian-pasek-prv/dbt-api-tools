import json

def get_prod_schemas_list(manifest_file="manifest.json"):
    """
    Returns a list of prod schemas from the manifest file.
    """
    with open(manifest_file, "r") as f:
        manifest = json.load(f)

    schemas = []
    nodes = manifest["nodes"]
    for node in nodes.values():
        schemas.append(node["schema"].upper())

    schemas = list(set(schemas))
    print(len(schemas))
    return schemas

def generate_where_in_statement(schemas):
    """
    Generates a SQL statement to filter by schemas.
    """
    where_in_statement = "WHERE schema IN ("
    for schema in schemas:
        where_in_statement += f"'{schema}',\n "
    where_in_statement = where_in_statement[:-2] + ")"
    return where_in_statement

# Output where statement into a txt file
with open("where_in_statement.txt", "w") as f:
    f.write(generate_where_in_statement(get_prod_schemas_list()))