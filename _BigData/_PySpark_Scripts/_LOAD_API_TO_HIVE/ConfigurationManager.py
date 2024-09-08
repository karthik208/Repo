import configparser
import os

class ConfigManager:
    def __init__(self):
        self.config = {}

    def load_config(self, file_path):
        """Loads the configuration file and handles exceptions."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Configuration file '{file_path}' not found.")

        parser = configparser.ConfigParser()

        try:
            parser.read(file_path)
            if not parser.sections():
                raise ValueError(f"No sections found in configuration file '{file_path}'.")

            # Iterate through the sections and load them into the dictionary
            for section in parser.sections():
                self.config[section] = {}
                for key in parser[section]:
                    self.config[section][key] = parser[section][key]

        except configparser.Error as e:
            # Handle parsing issues (e.g., file format errors)
            raise ValueError(f"Error parsing configuration file '{file_path}': {e}")

        except Exception as e:
            # Handle common issue
            raise Exception(f"Error parsing configuration file '{file_path}': {e}")

    def get_config_value(self, section, key, default=None):
        """Retrieves a value from a specific section and key, or returns a default value if not found."""
        if not self.config:
            raise Exception("Configuration is empty. Please call 'load_config' first.")

        try:
            # Try to retrieve the value
            return self.config[section][key]
        except KeyError:
            # Raise an exception or return a default value if section/key is not found
            if default is not None:
                return default
            raise KeyError(f"Key '{key}' not found in section '{section}'.")

    def get_all_configs(self):
        """Returns the entire config as a dictionary or raises an exception if config is empty."""
        if not self.config:
            raise Exception("Configuration is empty. Please call 'load_config' first.")
        return self.config

# region  Example usage of ConfigManage:
'''
try:
    config_manager = ConfigManager()
    config_manager.load_config('config.ini')  # Load the config file

    # Retrieve values
    hdfs_path = config_manager.get_config_value('config_API_TO_HDFS', 'hdfs.path')
    print(f"HDFS Path: {hdfs_path}")

    # Trying to retrieve a value that may not exist (with default value)
    unknown_value = config_manager.get_config_value('config_API_TO_HDFS', 'non.existent.key', default='default_value')
    print(f"Unknown Value: {unknown_value}")
except (FileNotFoundError, ValueError, KeyError) as e:
    print(f"Error: {e}")
'''
# endregion

# region Example Use Cases for Default Values in get_config_value:
'''
Let’s look at a few practical examples where default values come in handy.
1. Log Level Default
If your configuration file does not specify a logging level, you can default to a safer option like INFO.

Config File (config.ini):
ini code:
[logging]
log_level = DEBUG

python code:
log_level = config_manager.get_config_value('logging', 'log_level', default='INFO')
print(f"Log Level: {log_level}")

Output (if log_level exists in config):
Log Level: DEBUG
Output (if log_level is missing):
Log Level: INFO

2. Timeout Default
Let’s say you’re reading a configuration file that should provide a timeout value for network requests. If this value is missing, you want to default to 30 seconds.

python Code:
timeout = config_manager.get_config_value('network', 'timeout', default=30)
print(f"Timeout: {timeout} seconds")

Output (if timeout exists):
Timeout: 10 seconds
Output (if timeout is missing):
Timeout: 30 seconds  # Default value

3. API URL Fallback
If an API endpoint is not specified in the config file, you can default to a generic URL like "https://api.example.com".

python Code:
api_url = config_manager.get_config_value('api', 'url', default='https://api.example.com')
print(f"API URL: {api_url}")

Output (if api.url exists):
API URL: https://myapi.com/v1
Output (if api.url is missing):
API URL: https://api.example.com  # Default value
'''
# endregion

# region Example usage of get_all_configs
'''
all_configs = config_manager.get_all_configs()  # Call the method with parentheses
print("All Configuration Values:")
for section, values in all_configs.items():
    print(f"[{section}]")
    for key, value in values.items():
        print(f"{key} = {value}")
'''
# endregion