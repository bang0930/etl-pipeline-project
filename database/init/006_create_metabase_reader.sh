#!/usr/bin/env bash

set -euo pipefail

reader_user="${METABASE_ETL_DB_USER:-metabase_reader}"
reader_password="${METABASE_ETL_DB_PASSWORD:-metabase_reader}"

# PostgreSQL 식별자에 안전하게 사용할 수 있는 계정명만 허용한다.
if [[ ! "$reader_user" =~ ^[a-z_][a-z0-9_]*$ ]]; then
    echo "Invalid METABASE_ETL_DB_USER: use lowercase letters, numbers, and underscores." >&2
    exit 1
fi

# PostgreSQL 공식 이미지의 최초 초기화와 기존 DB용 일회성 서비스에서 함께 사용한다.
psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=ON_ERROR_STOP=1 \
    --set=reader_user="$reader_user" \
    --set=reader_password="$reader_password" <<'SQL'
SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L',
    :'reader_user',
    :'reader_password'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = :'reader_user'
) \gexec

-- 재실행 시에도 .env의 최신 비밀번호와 로그인 정책을 반영한다.
SELECT format(
    'ALTER ROLE %I WITH LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION',
    :'reader_user',
    :'reader_password'
) \gexec

SELECT format(
    'GRANT CONNECT ON DATABASE %I TO %I',
    current_database(),
    :'reader_user'
) \gexec

SELECT format('GRANT USAGE ON SCHEMA mart TO %I', :'reader_user') \gexec
SELECT format('GRANT SELECT ON ALL TABLES IN SCHEMA mart TO %I', :'reader_user') \gexec

-- 앞으로 ETL 소유자가 Mart 테이블을 추가해도 조회 권한이 자동 적용된다.
SELECT format(
    'ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO %I',
    :'reader_user'
) \gexec
SQL
