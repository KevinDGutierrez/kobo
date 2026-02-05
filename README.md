# KoBo → Dolibarr Integration (Test)

## Pasos rápidos
1. Editar index.js:
   - KOBO_TOKEN
   - ASSET_UID
   - DOLIBARR_API_KEY

2. Instalar:
   npm install

3. Ejecutar:
   npm run dev

## Qué hace
- Lee reportes de KoBo
- Busca ticket en Dolibarr por referencia
- Cambia estado a FINALIZADO (status=3)
