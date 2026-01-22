import os
from datetime import datetime
from typing import Optional, List, Dict, Any

import httpx
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt
from jose.exceptions import JWTError
from clickhouse_connect import get_client


app = FastAPI(title="BionicPRO Reports API")

# CORS нужен для вызовов из браузера (React UI).
# По умолчанию разрешаем локальную разработку; можно переопределить через CORS_ALLOW_ORIGINS.
cors_origins_env = os.getenv("CORS_ALLOW_ORIGINS")
if cors_origins_env:
    allow_origins = [o.strip() for o in cors_origins_env.split(",") if o.strip()]
else:
    allow_origins = ["http://localhost:3000", "http://127.0.0.1:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()

# ---- Keycloak / OIDC ----
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
KEYCLOAK_ISSUER = os.getenv(
    "KEYCLOAK_ISSUER",
    f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}",
)
JWKS_URL = os.getenv("JWKS_URL", f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs")

# ---- ClickHouse ----
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

MART_TABLE = os.getenv("MART_TABLE", "report_mart_user_telemetry_hourly")

# Важно: в витрине у тебя user_id вида u-001/u-002/...
# Для маппинга лучше использовать preferred_username (если username в Keycloak = u-001).
USER_ID_CLAIM = os.getenv("USER_ID_CLAIM", "preferred_username")


def ch_client():
    return get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )


_jwks_cache: Dict[str, Any] = {"value": None, "loaded_at": None}


async def get_jwks() -> Dict[str, Any]:
    """
    Упрощённый кэш JWKS ключей, чтобы не дергать Keycloak на каждый запрос.
    """
    ttl_seconds = int(os.getenv("JWKS_TTL_SECONDS", "300"))
    now = datetime.utcnow()

    cached = _jwks_cache.get("value")
    loaded_at: Optional[datetime] = _jwks_cache.get("loaded_at")

    if cached and loaded_at and (now - loaded_at).total_seconds() < ttl_seconds:
        return cached

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(JWKS_URL)
        resp.raise_for_status()
        jwks = resp.json()

    _jwks_cache["value"] = jwks
    _jwks_cache["loaded_at"] = now
    return jwks


async def get_current_user_id(
    cred: HTTPAuthorizationCredentials = Depends(security),
) -> str:
    token = cred.credentials
    jwks = await get_jwks()

    try:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        keys = jwks.get("keys", [])
        key = next((k for k in keys if k.get("kid") == kid), None)
        if not key:
            raise HTTPException(status_code=401, detail="Invalid token key id (kid)")

        # В учебном проекте чаще всего aud можно не проверять
        keycloak_audience = os.getenv("KEYCLOAK_AUDIENCE", None)

        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=keycloak_audience,
            issuer=KEYCLOAK_ISSUER,
            options={"verify_aud": False} if keycloak_audience is None else None,
        )

        # ✅ Важно для ограничения доступа "только себе":
        # user_id берём только из токена (не из query/params).
        user_id = claims.get(USER_ID_CLAIM)

        if not user_id:
            raise HTTPException(
                status_code=401,
                detail=f"Token has no '{USER_ID_CLAIM}' claim",
            )

        return str(user_id)

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


def rows_to_csv(rows: List[Dict[str, Any]]) -> str:
    """
    Простой CSV без внешних зависимостей.
    """
    if not rows:
        return "user_id,prosthesis_id,period_start,period_end,events_count,avg_signal_strength,max_signal_strength\n"

    headers = list(rows[0].keys())

    def esc(v: Any) -> str:
        if v is None:
            return ""
        s = str(v)
        if any(ch in s for ch in [",", '"', "\n", "\r"]):
            s = '"' + s.replace('"', '""') + '"'
        return s

    lines = []
    lines.append(",".join(headers))
    for r in rows:
        lines.append(",".join(esc(r.get(h)) for h in headers))
    return "\n".join(lines) + "\n"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/reports")
def get_report(
    user_id: str = Depends(get_current_user_id),
    format: str = Query("json", pattern="^(json|csv)$"),
):
    """
    Возвращает подготовленный отчёт из ClickHouse витрины.

    Ограничение доступа:
      - user_id берём только из токена (Depends(get_current_user_id)),
      - поэтому пользователь не может запросить отчёт другого пользователя.
    """
    sql = f"""
            SELECT
                user_id,
                prosthesis_id,
                period_start,
                period_end,

                -- Маппинг под контракт API (то, что ждёт фронт и CSV)
                telemetry_events_count AS events_count,
                avg_signal_level       AS avg_signal_strength,

                -- В витрине нет max_signal_strength.
                -- Для учебного проекта можно временно отдать avg как max,
                -- либо позже добавить реальный max в ETL и витрину.
                avg_signal_level       AS max_signal_strength

            FROM {MART_TABLE}
            WHERE user_id = %(user_id)s
            ORDER BY period_start DESC
            LIMIT 1000
        """
    params = {"user_id": user_id}

    client = ch_client()
    result = client.query(sql, parameters=params)

    rows = [dict(zip(result.column_names, row)) for row in result.result_rows]

    if format == "csv":
        csv = rows_to_csv(rows)
        return Response(
            content=csv,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="report.csv"'},
        )

    return JSONResponse(content={"user_id": user_id, "items": rows})
