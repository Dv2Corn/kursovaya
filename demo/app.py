"""HTTP-слой демо-сервиса ООО «Квартирант плюс» (только стандартная библиотека).

Запуск из корня репозитория:

    export DEMO_WEBHOOK_SECRET=change-me          # заглушка, не настоящий ключ
    export DEMO_ADMIN_TOKEN=change-me-owner       # необязательно: роль «владелец»
    export DEMO_MODERATOR_TOKEN=change-me-mod     # необязательно: роль «модератор»
    export DEMO_MESSENGER_SECRET=change-me-msg    # необязательно: оплата валютой мессенджера
    python3 -m demo.app

Эндпоинты (заголовки авторизации — в скобках):
    GET  /health                              состояние сервиса и базы
    POST /orders                              заказ подписки {"plan", "email"} или (X-Client-Key) для продления
    GET  /orders/<id>                         статус заказа (X-Client-Key)
    POST /webhooks/payment                    уведомление агрегатора (X-Signature: HMAC-SHA256 тела)
    POST /webhooks/messenger                  платёж валютой мессенджера (X-Messenger-Secret)
    GET  /catalog                             закрытый каталог (X-Client-Key, нужна активная подписка)
    GET  /catalog/<id>                        карточка объекта (X-Client-Key)
    POST /searches                            сохранить запрос {"district","price_max","rooms_min","rooms_max"}
    GET  /notifications                       уведомления клиента (X-Client-Key)
    POST /viewings                            запись на показ {"listing_id","slot_at"} (X-Client-Key)
    POST /listings                            агент заносит объект (X-Agent-Token)
    GET  /agent/viewings                      показы по объектам агента (X-Agent-Token)
    POST /viewings/<id>/confirm | /done       агент подтверждает / отмечает показ (X-Agent-Token)
    POST /listings/<id>/deal                  агент закрывает сделку {"commission"} (X-Agent-Token)
    POST /admin/agents                        завести агента (X-Admin-Token, владелец)
    GET  /admin/listings                      очередь модерации (владелец или модератор)
    POST /admin/listings/<id>/approve|reject  модерация (владелец или модератор)
    GET  /admin/metrics                       показатели для руководителя (владелец)
    GET  /admin/audit                         журнал действий (владелец)
    POST /admin/run-jobs                      проход планировщика вручную (владелец)
"""

from __future__ import annotations

import hmac
import json
import os
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Optional, Tuple

from demo.service import AgencyService, ServiceError

MAX_BODY = 64 * 1024

OWNER = "owner"
MODERATOR = "moderator"


def make_handler(service: AgencyService, admin_tokens: Dict[str, str]):
    tokens = {role: tok for role, tok in admin_tokens.items() if tok}

    class Handler(BaseHTTPRequestHandler):
        server_version = "AgencyDemo/1.0"
        sys_version = ""

        def log_message(self, fmt, *args):  # тише в тестах
            if os.environ.get("DEMO_ACCESS_LOG") == "1":
                super().log_message(fmt, *args)

        # --------------------------------------------------------- helpers
        def _send(self, status: int, payload) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def _raw_body(self) -> bytes:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                raise ServiceError(400, "bad_length")
            if length < 0 or length > MAX_BODY:
                raise ServiceError(413, "body_too_large")
            return self.rfile.read(length) if length else b""

        def _json(self) -> dict:
            try:
                data = json.loads(self._raw_body() or b"{}")
            except ValueError:
                raise ServiceError(400, "bad_json")
            if not isinstance(data, dict):
                raise ServiceError(400, "bad_json")
            return data

        def _role(self) -> Optional[str]:
            given = self.headers.get("X-Admin-Token", "").encode("utf-8")
            for role, tok in tokens.items():
                if given and hmac.compare_digest(given, tok.encode("utf-8")):
                    return role
            return None

        def _require_role(self, *allowed: str) -> str:
            role = self._role()
            if role not in allowed:
                raise ServiceError(403, "forbidden")
            return role

        def _route(self, method: str) -> Tuple[int, object]:
            path = self.path.split("?", 1)[0]
            h = self.headers
            client_key = h.get("X-Client-Key", "")
            agent_token = h.get("X-Agent-Token", "")

            def m(pattern: str) -> Optional[re.Match]:
                return re.fullmatch(pattern, path)

            if method == "GET" and path == "/health":
                return 200, service.health()

            # --- клиент: подписка и оплата
            if method == "POST" and path == "/orders":
                data = self._json()
                return 201, service.create_order(
                    str(data.get("plan", "")), email=data.get("email"), client_key=client_key or None)
            if method == "GET" and m(r"/orders/([\w-]+)"):
                return 200, service.get_order(m(r"/orders/([\w-]+)").group(1), client_key)
            if method == "POST" and path == "/webhooks/payment":
                res = service.handle_gateway_webhook(self._raw_body(), h.get("X-Signature", ""))
                return 200, {"result": res.result, "order_id": res.order_id}
            if method == "POST" and path == "/webhooks/messenger":
                res = service.handle_messenger_payment(h.get("X-Messenger-Secret", ""), self._raw_body())
                return 200, {"result": res.result, "order_id": res.order_id}

            # --- клиент: каталог, запросы, уведомления, показы
            if method == "GET" and path == "/catalog":
                return 200, {"listings": service.catalog(client_key)}
            if method == "GET" and m(r"/catalog/([\w-]+)"):
                return 200, service.listing_detail(client_key, m(r"/catalog/([\w-]+)").group(1))
            if method == "POST" and path == "/searches":
                return 201, service.save_search(client_key, self._json())
            if method == "GET" and path == "/notifications":
                return 200, {"notifications": service.notifications_for(client_key)}
            if method == "POST" and path == "/viewings":
                data = self._json()
                return 201, service.request_viewing(client_key, str(data.get("listing_id", "")), data.get("slot_at"))

            # --- агент
            if method == "POST" and path == "/listings":
                return 201, service.submit_listing(agent_token, self._json())
            if method == "GET" and path == "/agent/viewings":
                return 200, {"viewings": service.agent_viewings(agent_token)}
            vm = m(r"/viewings/([\w-]+)/(confirm|done)")
            if method == "POST" and vm:
                action = service.confirm_viewing if vm.group(2) == "confirm" else service.complete_viewing
                return 200, action(agent_token, vm.group(1))
            dm = m(r"/listings/([\w-]+)/deal")
            if method == "POST" and dm:
                return 200, service.close_deal(agent_token, dm.group(1), self._json().get("commission"))

            # --- администрирование: роли «владелец» и «модератор»
            if path.startswith("/admin/"):
                if method == "POST" and path == "/admin/agents":
                    role = self._require_role(OWNER)
                    return 201, service.create_agent(str(self._json().get("name", "")), actor=role)
                if method == "GET" and path == "/admin/listings":
                    self._require_role(OWNER, MODERATOR)
                    return 200, {"listings": service.moderation_queue()}
                mm = m(r"/admin/listings/([\w-]+)/(approve|reject)")
                if method == "POST" and mm:
                    role = self._require_role(OWNER, MODERATOR)
                    if mm.group(2) == "approve":
                        return 200, service.approve_listing(mm.group(1), moderator=role)
                    return 200, service.reject_listing(mm.group(1), self._json().get("reason", ""), moderator=role)
                if method == "GET" and path == "/admin/metrics":
                    self._require_role(OWNER)
                    return 200, service.metrics()
                if method == "GET" and path == "/admin/audit":
                    self._require_role(OWNER)
                    return 200, {"entries": service.audit_entries()}
                if method == "POST" and path == "/admin/run-jobs":
                    self._require_role(OWNER)
                    return 200, service.run_jobs()
                self._require_role(OWNER, MODERATOR)
            raise ServiceError(404, "not_found")

        def _handle(self, method: str) -> None:
            try:
                status, payload = self._route(method)
            except ServiceError as exc:
                status, payload = exc.status, {"error": exc.code}
            except Exception:  # pragma: no cover - детали ошибки клиенту не раскрываем
                status, payload = 500, {"error": "internal"}
            self._send(status, payload)

        def do_GET(self):
            self._handle("GET")

        def do_POST(self):
            self._handle("POST")

    return Handler


def build_server(
    service: AgencyService,
    host: str = "127.0.0.1",
    port: int = 8000,
    admin_token: Optional[str] = None,
    moderator_token: Optional[str] = None,
) -> ThreadingHTTPServer:
    tokens = {OWNER: admin_token or "", MODERATOR: moderator_token or ""}
    return ThreadingHTTPServer((host, port), make_handler(service, tokens))


def start_scheduler(service: AgencyService, interval_seconds: float) -> threading.Event:
    """Фоновый планировщик: сверка, сроки подписок, напоминания, отправка уведомлений."""
    stop = threading.Event()

    def loop():
        while not stop.wait(interval_seconds):
            try:
                report = service.run_jobs()
                if report["reconcile"]["checked"] or report["notifications"]["sent"]:
                    print(f"[планировщик] {report}", flush=True)
            except Exception as exc:  # pragma: no cover
                print(f"[планировщик] ошибка: {exc!r}", file=sys.stderr, flush=True)

    threading.Thread(target=loop, name="scheduler", daemon=True).start()
    return stop


def main() -> int:
    secret = os.environ.get("DEMO_WEBHOOK_SECRET", "")
    if not secret:
        print(
            "Не задан DEMO_WEBHOOK_SECRET. Для локального запуска: export DEMO_WEBHOOK_SECRET=change-me",
            file=sys.stderr,
        )
        return 2
    db_path = os.environ.get("DEMO_DB_PATH", "demo-agency.db")
    port = int(os.environ.get("DEMO_PORT", "8000"))
    service = AgencyService(
        db_path,
        secret,
        messenger_secret=os.environ.get("DEMO_MESSENGER_SECRET") or None,
        pending_ttl_seconds=int(os.environ.get("DEMO_PENDING_TTL", str(30 * 60))),
    )
    stop = start_scheduler(service, float(os.environ.get("DEMO_JOBS_INTERVAL", "60")))
    server = build_server(
        service,
        port=port,
        admin_token=os.environ.get("DEMO_ADMIN_TOKEN") or None,
        moderator_token=os.environ.get("DEMO_MODERATOR_TOKEN") or None,
    )
    print(f"Демо «Квартирант плюс»: http://127.0.0.1:{port}/health  (база: {db_path})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
        service.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
