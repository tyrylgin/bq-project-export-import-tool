from google.cloud import bigquery, bigquery_datatransfer_v1
from typing import Dict, Any

class BigQueryUtils:
    """Utility class for serializing BigQuery objects."""

    @staticmethod
    def serialize_routine(routine: bigquery.Routine) -> Dict[str, Any]:
        return {
            'routine_id': routine.routine_id,
            'type_': routine.type_,
            'language': routine.language,
            'body': routine.body,
            'arguments': [
                {
                    'name': arg.name,
                    'data_type': arg.data_type.to_api_repr(),
                    'mode': arg.mode
                } for arg in routine.arguments
            ],
            'created': routine.created.isoformat() if routine.created else None,
            'modified': routine.modified.isoformat() if routine.modified else None,
            'description': routine.description
        }

    @staticmethod
    def serialize_view(view: bigquery.Table) -> Dict[str, Any]:
        return {
            'table_id': view.table_id,
            'view_query': view.view_query,
            'description': view.description,
            'created': view.created.isoformat() if view.created else None,
            'modified': view.modified.isoformat() if view.modified else None,
            'schema': [field.to_api_repr() for field in view.schema]
        }

    @staticmethod
    def serialize_external_table(ext_table: bigquery.Table) -> Dict[str, Any]:
        return {
            'table_id': ext_table.table_id,
            'schema': [field.to_api_repr() for field in ext_table.schema],
            'external_data_configuration': ext_table.external_data_configuration.to_api_repr(),
            'description': ext_table.description,
            'created': ext_table.created.isoformat() if ext_table.created else None,
            'modified': ext_table.modified.isoformat() if ext_table.modified else None
        }

    @staticmethod
    def serialize_scheduled_query(transfer: bigquery_datatransfer_v1.TransferConfig) -> Dict[str, Any]:
        def serialize_message(message):
            if hasattr(message, '__dict__'):
                return {k: serialize_message(v) for k, v in message.__dict__.items() if not k.startswith('_')}
            elif isinstance(message, (list, tuple)):
                return [serialize_message(v) for v in message]
            elif isinstance(message, dict):
                return {k: serialize_message(v) for k, v in message.items()}
            else:
                return message

        serialized = {
            'name': transfer.name,
            'display_name': transfer.display_name,
            'data_source_id': transfer.data_source_id,
            'params': serialize_message(transfer.params),
            'params_query': transfer.params.get('query') if transfer.params else None,
            'schedule': transfer.schedule,
            'schedule_options': serialize_message(transfer.schedule_options) if transfer.schedule_options else None,
            'destination_dataset_id': transfer.destination_dataset_id,
            'disabled': transfer.disabled,
            'update_time': transfer.update_time.isoformat() if transfer.update_time else None,
            'next_run_time': transfer.next_run_time.isoformat() if transfer.next_run_time else None,
            'state': transfer.state.name if transfer.state else None,
            'user_id': transfer.user_id,
            'dataset_region': transfer.dataset_region,
            'notification_pubsub_topic': transfer.notification_pubsub_topic,
            'email_preferences': serialize_message(transfer.email_preferences) if transfer.email_preferences else None,
        }
        return serialized
