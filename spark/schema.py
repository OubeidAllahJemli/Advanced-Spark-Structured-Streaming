"""
This file defines the expected schema of the streaming JSON events.

You will use this schema in streaming_app.py to:
- Parse raw JSON messages
- Enable event-time processing
- Detect malformed or incomplete records
"""

from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    DoubleType, 
    TimestampType
)


# Expected schema for valid events
# All fields are nullable since we're dealing with dirty data
EVENT_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("event_time", StringType(), True),  # Will convert to timestamp later
    StructField("temperature", DoubleType(), True),
    StructField("country", StringType(), True)
])


# Valid country codes (for data quality validation)
VALID_COUNTRIES = {'FR', 'USA', 'France', 'france', 'United States', 'Germany', 'Germnay'}


# Temperature thresholds for validation
TEMP_MIN = -50.0  # Minimum realistic temperature
TEMP_MAX = 60.0   # Maximum realistic temperature
TEMP_INVALID_VALUE = -999  # Sentinel value used for missing data


def is_valid_record(row):
    """
    Check if a record passes all validation rules.
    
    Validation rules:
    1. device_id must not be null or empty
    2. event_time must not be null or empty
    3. temperature must be present and valid (not -999)
    4. temperature must be within realistic range
    5. country must not be null or empty
    6. country must not contain special characters or numbers
    
    Args:
        row: A Row object from Spark DataFrame
        
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        # Check device_id
        if not row.device_id or row.device_id.strip() == '':
            return False
        
        # Check event_time
        if not row.event_time or row.event_time.strip() == '':
            return False
        
        # Check temperature exists
        if row.temperature is None:
            return False
        
        # Check temperature is not sentinel value
        if row.temperature == TEMP_INVALID_VALUE:
            return False
        
        # Check temperature is in valid range
        if row.temperature < TEMP_MIN or row.temperature > TEMP_MAX:
            return False
        
        # Check country
        if not row.country or row.country.strip() == '':
            return False
        
        # Check country doesn't contain invalid characters
        country = row.country.strip()
        if any(char.isdigit() or char in '@#$%^&*()' for char in country):
            return False
        
        return True
        
    except Exception:
        return False


def get_validation_reason(row):
    """
    Get the reason why a record is invalid.
    
    Args:
        row: A Row object from Spark DataFrame
        
    Returns:
        str: Description of validation failure
    """
    try:
        if not row.device_id or row.device_id.strip() == '':
            return "missing_device_id"
        
        if not row.event_time or row.event_time.strip() == '':
            return "missing_event_time"
        
        if row.temperature is None:
            return "missing_temperature"
        
        if row.temperature == TEMP_INVALID_VALUE:
            return "invalid_sentinel_temperature"
        
        if row.temperature < TEMP_MIN or row.temperature > TEMP_MAX:
            return "temperature_out_of_range"
        
        if not row.country or row.country.strip() == '':
            return "missing_country"
        
        country = row.country.strip()
        if any(char.isdigit() or char in '@#$%^&*()' for char in country):
            return "invalid_country_format"
        
        return "valid"
        
    except Exception as e:
        return f"validation_error: {str(e)}"
