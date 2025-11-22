from datetime import datetime
from common.utils.logger_config import logger

# Data validation
def validate_data(data_name, data, required_fields):
    if required_fields is None:
        required_fields = set()
        
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'data_name': data_name,
        'records_count': len(data),
    }
    
    has_required_fields = True
    if required_fields and data:
        has_required_fields = all(required_fields.issubset(record.keys()) for record in data)
    elif required_fields and not data:
        has_required_fields = False
    
    status = 'PASS'
    if len(data) == 0:
        if data_name == 'live_football' or data_name == 'recent_finish_football':
            status = 'PASS (Zero allowed)' 
        else:
            status = 'FAIL (Zero Count)'
    
    validation_results['status'] = status
    validation_results['has_required_fields'] = has_required_fields
    
    failed_conditions = []
    if status.startswith('FAIL'):
        failed_conditions.append(status)
    if not has_required_fields and required_fields:
        failed_conditions.append(f"Missing one or more required fields: {required_fields}")

    logger.info(f"Data validation completed for {data_name}: {validation_results}") 
    
    if failed_conditions:
        logger.error(f"Validation failed for {data_name}. Failed conditions: {', '.join(failed_conditions)}")
        raise ValueError(f"Data validation failed for {data_name}. Reasons: {', '.join(failed_conditions)}")
    
    return validation_results