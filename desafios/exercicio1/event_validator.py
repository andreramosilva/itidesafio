import json
import boto3

_SQS_CLIENT = None
schema = open('schema.json')
schema = json.load(schema)
fields_accepted = schema['required']

def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")

def handler(event):
    '''
    #  Função principal que é sensibilizada para cada evento
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função send_event_to_queue para envio do evento para a fila,
        não é necessário alterá-la
    '''
    # validaçoes basicas dos campos, se os tipos sao compativeis, se sao campos validos e se a quantidade de campos esta correta.
    if check_field_types(event) and check_valid_fields(event) and check_quantity_fields(event):
        return event    
    else:
        raise Exception("Date provided does not match the criteria(field types or has fields not accepteds/missing fields)")


def check_valid_fields(event): 
    checado = set()
    for x in event:
        if x not in fields_accepted:
            print("Nao Valido campo nao faz parte dos campos aceitos")
            return False
        checado.add(x)
    return True

def check_quantity_fields(event):
    return len(event)==len(fields_accepted)    


def check_field_types(event):
    address = schema["properties"]["address"]["required"]
    for x in fields_accepted:        
        if x == "address":
            for y in x:
                if y not in address:
                    return False
            continue

        if (type(event[x]) == type(schema["properties"][x]["examples"][0])) != True:
            print("INVALIDO")
            return False

    return True