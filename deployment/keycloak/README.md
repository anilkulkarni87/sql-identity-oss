# Keycloak Bootstrap

This folder contains realm import assets used by the enterprise compose stack.

## Included Assets

- `idr-realm.json`: bootstrap realm import for:
  - realm: `idr-realm`
  - client: `idr-web`
  - default test user: `test` / `test`

## Usage

`docker-compose.enterprise.yml` mounts this file to:

`/opt/keycloak/data/import/idr-realm.json`

and starts Keycloak with:

`start-dev --import-realm`
