import json
import logging

logging.basicConfig(level=logging.INFO)

def process_message(**context):
    message = context['ti'].xcom_pull(key='retrieved_message', task_ids='recevie_message')
    logging.info(f"Processing message: {message}")
    try:
        message_data = json.loads(message)
        context['ti'].xcom_push(key='json_data', value=message_data)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON message: {message}, Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error processing message: {e}")