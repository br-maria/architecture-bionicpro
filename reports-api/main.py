import os
from datetime import datetime
from typing import Optional, List, Dict, Any

import httpx
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import Response, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt
from jose.exceptions import JWTError
from clickhouse_connect import get_client


app = FastAPI(title="BionicPRO Reports API")
security = HTTPBearer()

KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
KEYCLOAK_ISSUER = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}"

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

MART_TABLE = os.getenv("MART_TABLE", "report_mart_user_telemetry_hourly")

# Важно: в витрине у тебя user_id вида u-001/u-002/...
# Для маппинга лучше использовать preferred_username (если username в Keycloak = u-001).
USER_ID_CLAIM = os.getenv("USER_ID_CLAIM", "preferred_username")

_jwks_cache: Optional[Dict[str, Any]] = None


async def get_jwks() -> Dict[str, Any]:
    global _jwks_cache
    if _jwks_cache:
        return _jwks_cache
    url = f"{KEYCLOAK_ISSUER}/protocol/openid-connect/certs"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
        r.raise_for_status()
        _jwks_cache = r.json()
        return _jwks_cache


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


def ch_client():
    return get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
    )


def rows_to_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return (
            "period_start,period_end,user_id,prosthesis_id,crm_client_name,crm_email,crm_country,"
            "telemetry_events_count,movements_count,avg_signal_level,avg_noise_level,errors_count,battery_low_events\n"
        )
    cols = list(rows[0].keys())
    lines = [",".join(cols)]
    for r in rows:
        lines.append(",".join([str(r.get(c, "")) for c in cols]))
    return "\n".join(lines) + "\n"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/reports")
def get_report(
    # ✅ user_id только из токена — так пользователь не сможет запросить чужие отчёты
    user_id: str = Depends(get_current_user_id),
    from_dt: Optional[datetime] = Query(default=None, alias="from"),
    to_dt: Optional[datetime] = Query(default=None, alias="to"),
    format: str = Query(default="json", pattern="^(json|csv)$"),
):
    where = ["user_id = %(user_id)s"]
    params = {"user_id": user_id}

    if from_dt:
        where.append("period_start >= %(from_dt)s")
        params["from_dt"] = from_dt
    if to_dt:
        where.append("period_end <= %(to_dt)s")
        params["to_dt"] = to_dt

    sql = f"""
        SELECT
          period_start, period_end, user_id, prosthesis_id,
          crm_client_name, crm_email, crm_country,
          telemetry_events_count, movements_count, avg_signal_level, avg_noise_level,
          errors_count, battery_low_events
        FROM {MART_TABLE}
        WHERE {" AND ".join(where)}
        ORDER BY period_start DESC
        LIMIT 5000
    """

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
