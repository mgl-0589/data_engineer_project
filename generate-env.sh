#!/bin/bash

if ! grep -q "AIRFLOW_UID" .env; then
	echo "" >> .env
	echo "AIRFLOW_UID=$(id -u)" >> .env
fi
