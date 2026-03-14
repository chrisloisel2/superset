-- Création de la base Superset si elle n'existe pas
SELECT 'CREATE DATABASE superset OWNER hive'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec
