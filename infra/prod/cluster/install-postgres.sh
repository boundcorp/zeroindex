#!/usr/bin/env bash

helm upgrade --install postgres oci://registry-1.docker.io/bitnamicharts/postgresql \
    --set auth.postgresPassword=zeroindex \
    --set auth.database=zeroindex \
    --set auth.username=zeroindex \
    --set auth.password=zeroindex \
    --set auth.rootPassword=zeroindex \
    --set primary.persistence.enabled=true \
    --set primary.persistence.size=1Gi
