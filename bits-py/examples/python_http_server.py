#!/usr/bin/env python3
"""Minimal asyncio-native HTTP server built on top of bits_py.

Run with:
  python bits-py/examples/python_http_server.py
"""

import asyncio
from aiohttp import web

from bits_py import Bits


BIND_HOST = "0.0.0.0"
BIND_PORT = 8080
POLL_TIMEOUT_SECS = 25.0

CONFIG = """
routes:
  default:
    - target::http:
        url: http://localhost:8081
"""


async def submit_job(request: web.Request) -> web.StreamResponse:
    bits = request.app["bits"]
    payload = await request.json()
    job_id = await bits.submit(payload)
    return await poll_by_id(job_id, bits)


async def poll_job(request: web.Request) -> web.StreamResponse:
    bits = request.app["bits"]
    job_id = request.match_info["id"]
    return await poll_by_id(job_id, bits)


async def poll_by_id(job_id: str, bits: Bits) -> web.StreamResponse:
    outcome = await bits.poll(job_id, timeout_secs=POLL_TIMEOUT_SECS)
    status = outcome["status"]

    if status == "ready":
        return result_to_response(outcome["result"])

    if status == "pending":
        raise web.HTTPSeeOther(
            location=f"/job/{outcome['id']}",
            headers={"Retry-After": "0"},
        )

    if status == "not_found":
        return web.Response(status=404)

    if status == "job_lost":
        return web.Response(status=410)

    return web.Response(status=500, text=f"unexpected poll status: {status}")


def result_to_response(result: dict) -> web.StreamResponse:
    status = result["status"]

    if status == "success":
        return web.Response(
            body=result["body"],
            headers={"Content-Type": result["content_type"]},
        )

    if status == "redirect":
        raise web.HTTPSeeOther(location=result["location"])

    if status == "error":
        return web.Response(status=400, text=result["message"])

    if status == "failed":
        return web.Response(status=500, text=result["reason"])

    if status in ("cancelled", "client_gone"):
        return web.Response(status=410)

    return web.Response(status=500, text=f"unexpected result status: {status}")


async def create_app() -> web.Application:
    bits = await Bits.from_config(CONFIG)
    app = web.Application()
    app["bits"] = bits
    app.add_routes(
        [
            web.post("/job", submit_job),
            web.get("/job/{id}", poll_job),
        ]
    )
    return app


def main() -> None:
    app = asyncio.run(create_app())
    web.run_app(app, host=BIND_HOST, port=BIND_PORT)


if __name__ == "__main__":
    main()
