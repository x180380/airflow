from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from typing import Any, Sequence
import json
from datetime import datetime


def apply_function(message, arrival_date_value=None):
    if message:
        if arrival_date_value is None:
            return message.value()
        try:
            # Parse the message value as JSON
            message_data = json.loads(message.value())
            
            # Check if 'arrival_date' exists in the JSON
            if 'arrival_date' in message_data:
                arrival_date_str = message_data['arrival_date']
                
                # Parse dates in yyyy-MM-dd format
                arrival_date = datetime.strptime(arrival_date_str, '%Y-%m-%d').date()
                user_date = datetime.strptime(arrival_date_value, '%Y-%m-%d').date()
                
                # Return message value only if arrival_date >= user_date
                if arrival_date >= user_date:
                    return message.value()
            else:
                return message.value()
                
        except (json.JSONDecodeError, ValueError, KeyError):
            # If JSON parsing fails or date format is invalid, return None
            pass
    
    return None

class AwaitMessageSensor(AwaitMessageSensor):
    def __init__(
        self,
        topics: Sequence[str],
        apply_function: str | None = None,
        kafka_config_id: str = "kafka_default",
        apply_function_args: Sequence[Any] | None = None,
        apply_function_kwargs: dict[Any, Any] | None = None,
        poll_timeout: float = 1,
        poll_interval: float = 5,
        xcom_push_key=None,
        arrival_date_value: str | None = None,
        **kwargs: Any,
    ) -> None:
        if apply_function is None:
            apply_function = self.__class__.__module__ + ".apply_function"
        
        # Add arrival_date_value to apply_function_kwargs
        if apply_function_kwargs is None:
            apply_function_kwargs = {}
        if arrival_date_value:
            apply_function_kwargs['arrival_date_value'] = arrival_date_value
        
        super().__init__(
            topics=topics,
            apply_function=apply_function,
            kafka_config_id=kafka_config_id,
            apply_function_args=apply_function_args,
            apply_function_kwargs=apply_function_kwargs,
            poll_timeout=poll_timeout,
            poll_interval=poll_interval,
            xcom_push_key=xcom_push_key,
            **kwargs,
        )
