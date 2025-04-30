from dotenv import load_dotenv
import os
import snowflake.connector

def load_custom_env(username):
    env_file = f".env.{username}"
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        raise FileNotFoundError(f"Environment file {env_file} not found.")
    
load_custom_env('rocha')