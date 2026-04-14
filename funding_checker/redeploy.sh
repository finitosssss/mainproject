#!/bin/bash
# Complete Re-deploy script for Azure Container Instance
# This script builds the image in ACR and updates the ACI via YAML configuration.

source /Users/macbook/.gemini/antigravity/scratch/mainproject-main/funding_checker/.venv/bin/activate
AZ_PATH="/Users/macbook/.gemini/antigravity/scratch/mainproject-main/funding_checker/.venv/bin/az"
PY_PATH="/Users/macbook/.gemini/antigravity/scratch/mainproject-main/funding_checker/.venv/bin/python3"

echo "--- 1/4 Building fresh image in ACR ---"
$AZ_PATH acr build --registry checkers --image fundingchecker:latest .

echo "--- 2/4 Generating deployment YAML ---"
ACR_PASSWORD=$($AZ_PATH acr credential show --name checkers --query 'passwords[0].value' --output tsv)
$PY_PATH generate_aci_yaml.py "$ACR_PASSWORD"

echo "--- 3/4 Deleting old container instance ---"
$AZ_PATH container delete --resource-group checkers --name checkers --yes

echo "--- 4/4 Creating fresh container from YAML file ---"
$AZ_PATH container create --resource-group checkers --file final_deployment.yaml

echo "--- DEPLOYMENT COMPLETE! ---"
# Clean up sensitive file
rm final_deployment.yaml
