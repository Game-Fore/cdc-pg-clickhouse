@'
$ErrorActionPreference = "Stop"

docker compose down -v
docker compose up -d --wait

Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors `
  -ContentType "application/json" -InFile .\connectors\postgres-shop.json | Out-Null

Start-Sleep -Seconds 5
Invoke-RestMethod http://localhost:8083/connectors/shop-connector/status | ConvertTo-Json -Depth 5
'@ | Set-Content -Path .\reset.ps1 -Encoding UTF8