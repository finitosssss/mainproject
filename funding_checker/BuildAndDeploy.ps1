# Build and Deploy Docker Image to Azure Container Instance
# Update these variables
$acrName = "checkers-f8ezc5dwg9cvceeh"           # e.g., myregistry
$acrLoginServer = "$acrName.azurecr.io"
$imageName = "fundingchecker"       # e.g., fundingchecker
$imageTag = "latest"
$resourceGroup = "checkers"
$aciName = "checkers"

# 1. Build Docker image
docker build -t ${acrLoginServer}/${imageName}:${imageTag} .

# 2. Login to Azure and ACR
az acr login --name $acrName

# 3. Push image to ACR
docker push ${acrLoginServer}/${imageName}:${imageTag}

# 4. Delete existing Azure Container Instance (if it exists)
az container delete `
    --resource-group $resourceGroup `
    --name $aciName `
    --yes

# 5. Deploy to Azure Container Instance
az container create `
    --resource-group $resourceGroup `
    --name $aciName `
    --image ${acrLoginServer}/${imageName}:${imageTag} `
    --registry-login-server $acrLoginServer `
    --registry-username $(az acr credential show --name $acrName --query username --output tsv) `
    --registry-password $(az acr credential show --name $acrName --query passwords[0].value --output tsv) `
    --os-type Linux `
    --cpu 2 `
    --memory 3 `
    --restart-policy Always `
    --environment-variables `
        PYTHONUNBUFFERED=1 `
        REQUESTS_TIMEOUT=30 `
        AIOHTTP_TIMEOUT=30

Write-Host "Deployment complete."