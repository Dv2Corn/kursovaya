"""HTTP-слой демо-сервиса подписок (только стандартная библиотека).

Запуск из корня репозитория:

    export DEMO_WEBHOOK_SECRET=change-me      # заглушка, не настоящий ключ
    export DEMO_ADMIN_TOKEN=change-me-too     # необязательно: включает /admin/*
    python3 -m demo.app

Эндпоинты:
    GET  /health                    состояние сервиса и базы
    POST /orders                    создать заказ {"email": ..., "plan": "1m|3m|12m"}
    GET  /orders/<id>               статус заказа (заголовок X-Order-Key), токен — один раз
    POST /webhooks/payment          уведомление агрегатора (заголовок X-Signature)
    GET  /admin/audit               журнал действий (заголовок X-Admin-Token)
    POST /admin/reconcile           сверка зависших заказов вручную (X-Admin-Token)
"""

from __future__ import annotations

import hmac
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional, Tuple

from demo.service import ServiceError, SubscriptionService

MAX_BODY = 64 * 1024


def make_handler(service: SubscriptionService, admin_token: Optional[str]):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ServiceDemo/1.0"
        sys_version = ""

        def log_message(self, fmt, *args):  # noqa: D401 - тише в тестах
            if os.environ.get("DEMO_ACCESS_LOG") == "1":
                super().log_message(fmt, *args)

        # --------------------------------------------------------- helpers
        def _send(self, status: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> bytes:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                raise ServiceError(400, "bad_length")
            if length < 0 or length > MAX_BODY:
                raise ServiceError(413, "body_too_large")
            return self.rfile.read(length) if length else b""

        def _admin_ok(self) -> bool:
            if not admin_token:
                return False
            given = self.headers.get("X-Admin-Token", "")
            return hmac.compare_digest(given.encode("utf-8"), admin_token.encode("utf-8"))

        def _route(self, method: str) -> Tuple[int, dict]:
            path = self.path.split("?", 1)[0]
            if method == "GET" and path == "/health":
                return 200, service.health()
            if method == "POST" and path == "/orders":
                try:
                    data = json.loads(self._read_body() or b"{}")
                except ValueError:
                    raise ServiceError(400, "bad_json")
                if not isinstance(data, dict):
                    raise ServiceError(400, "bad_json")
                return 201, service.create_order(str(data.get("email", "")), str(data.get("plan", "")))
            if method == "GET" and path.startswith("/orders/"):
                order_id = path[len("/orders/"):]
                return 200, service.get_order_for_customer(order_id, self.headers.get("X-Order-Key", ""))
            if method == "POST" and path == "/webhooks/payment":
                body = self._read_body()
                res = service.handle_webhook(body, self.headers.get("X-Signature", ""))
                return 200, {"result": res.result, "order_id": res.order_id}
            if path.startswith("/admin/"):
                if not self._admin_ok():
                    raise ServiceError(403, "forbidden")
                if method == "GET" and path == "/admin/audit":
                    return 200, {"entries": service.audit_entries()}
                if method == "POST" and path == "/admin/reconcile":
                    return 200, service.reconcile_stale_orders()
            raise ServiceError(404, "not_found")

        def _handle(self, method: str) -> None:
            try:
                status, payload = self._route(method)
            except ServiceError as exc:
                status, payload = exc.status, {"error": exc.code}
            except Exception:  # pragma: no cover - не раскрываем детали клиенту
                status, payload = 500, {"error": "internal"}
            self._send(status, payload)

        def do_GET(self):
            self._handle("GET")

        def do_POST(self):
            self._handle("POST")

    return Handler


def build_server(
    service: SubscriptionService,
    host: str = "127.0.0.1",
    port: int = 8000,
    admin_token: Optional[str] = None,
) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(service, admin_token))


def start_scheduler(service: SubscriptionService, interval_seconds: float) -> threading.Event:
    """Фоновая сверка зависших заказов раз в interval_seconds."""
    stop = threading.Event()

    def loop():
        while not stop.wait(interval_seconds):
            try:
                report = service.reconcile_stale_orders()
                if report["checked"]:
                    print(f"[сверка] {report}", flush=True)
            except Exception as exc:  # pragma: no cover
                print(f"[сверка] ошибка: {exc!r}", file=sys.stderr, flush=True)

    threading.Thread(target=loop, name="reconcile", daemon=True).start()
    return stop


def main() -> int:
    secret = os.environ.get("DEMO_WEBHOOK_SECRET", "")
    if not secret:
        print(
            "Не задан DEMO_WEBHOOK_SECRET. Для локального запуска: export DEMO_WEBHOOK_SECRET=change-me",
            file=sys.stderr,
        )
        return 2
    db_path = os.environ.get("DEMO_DB_PATH", "demo-service.db")
    port = int(os.environ.get("DEMO_PORT", "8000"))
    service = SubscriptionService(
        db_path,
        secret,
        pending_ttl_seconds=int(os.environ.get("DEMO_PENDING_TTL", str(30 * 60))),
    )
    stop = start_scheduler(service, float(os.environ.get("DEMO_RECONCILE_INTERVAL", "60")))
    server = build_server(service, port=port, admin_token=os.environ.get("DEMO_ADMIN_TOKEN") or None)
    print(f"Демо-сервис подписок: http://127.0.0.1:{port}/health  (база: {db_path})", flush=True)
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
