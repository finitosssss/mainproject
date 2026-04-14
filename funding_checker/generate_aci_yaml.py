import os
import yaml
from dotenv import load_dotenv
import sys

def generate_yaml(template_path, env_path, output_path, acr_password):
    # Load .env into dictionary
    load_dotenv(env_path)
    
    # Read template
    with open(template_path, 'r') as f:
        config = yaml.safe_load(f)
        
    # Prepare environment variables list
    # These are the ones we want to pass to the container
    env_vars = [
        "MONGO_URI", "MONGO_DB_NAME_HEDGE_STRATEGY", "MONGO_COLLECTION_NAME_HEDGE_STRATEGY",
        "MONGO_DB_NAME_FUNDING_MONITOR", "MONGO_COLLECTION_NAME_FUNDING_MONITOR",
        "MONGO_DB_NAME_VOLUME_TRACKER", "MONGO_COLLECTION_NAME_VOLUME_TRACKER",
        "MONGO_DB_NAME_TRADING_TOOLS", "MONGO_COLLECTION_NAME_TRADING_TOOLS",
        "VOLUME_TRACKER_TOKEN", "VOLUME_TRACKER_CHAT_IDS",
        "TRADING_TOOLS_TOKEN", "TRADING_TOOLS_CHAT_IDS",
        "UNIQUE_STRATEGY_TOKEN", "UNIQUE_STRATEGY_CHAT_IDS",
        "MONGO_DB_NAME_UNIQUE_STRATEGY", "MONGO_COLLECTION_NAME_UNIQUE_STRATEGY",
        "FUNDING_BOT_TOKEN", "HEDGE_BOT_TOKEN", "TELEGRAM_CHAT_ID",
        "BYBIT_API_KEY", "BYBIT_API_SECRET"
    ]
    
    final_env = []
    # Add defaults
    final_env.append({"name": "PYTHONUNBUFFERED", "value": "1"})
    final_env.append({"name": "REQUESTS_TIMEOUT", "value": "30"})
    final_env.append({"name": "AIOHTTP_TIMEOUT", "value": "30"})
    
    for var in env_vars:
        val = os.getenv(var)
        if val:
            final_env.append({"name": var, "value": val})
            
    # Update config
    config['properties']['containers'][0]['properties']['environmentVariables'] = final_env
    
    # Update ACR password
    if 'imageRegistryCredentials' in config['properties']:
        config['properties']['imageRegistryCredentials'][0]['password'] = acr_password
        
    # Write output
    with open(output_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    print(f"Generated {output_path} with {len(final_env)} environment variables.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_aci_yaml.py <acr_password>")
        sys.exit(1)
        
    acr_password = sys.argv[1]
    template = "aci_template.yaml"
    env = "../.env"
    output = "final_deployment.yaml"
    
    generate_yaml(template, env, output, acr_password)
