import json


_ATHENA_CLIENT = None
schema = open('schema.json')
schema = json.load(schema)
fields_accepted = schema['required']

def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''
    
    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )

def handler():
    '''
    #  Função principal
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função create_hive_table_with_athena para te auxiliar
        na criação da tabela HIVE, não é necessário alterá-la
    '''
    
    create_hive_table_with_athena(create_table_script())

def create_table_script():
    query = "CREATE EXTERNAL TABLE IF NOT EXISTS SPC_TABLE ("

    for x in fields_accepted:
        tipo = schema["properties"][x]["type"]
        if tipo=="integer":
            tipo = "int"
        if x == "address":
            query+=x+" string"
            continue
        if x == fields_accepted[-1]:
            query+=x+" "+tipo
            continue
        query+=x+" "+tipo+","

    query = query + r''')
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    LOCATION 's3://iti-query-results/'
    '''

    return query