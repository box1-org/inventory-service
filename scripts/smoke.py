#!/usr/bin/env python3
"""Smoke-test the live HTTPS Route, preserving certificate verification."""
import json
import os
import re
import sys
import urllib.error
import urllib.request

base = sys.argv[1].rstrip("/")
prefix = os.environ.get("EXPECTED_PREFIX", "Hello from box1")

def get(path, accept="application/json"):
    req = urllib.request.Request(base + path, headers={"Accept": accept})
    with urllib.request.urlopen(req, timeout=20) as response:
        assert response.status == 200, (path, response.status)
        return response.read().decode()

greeting = json.loads(get("/api/greetings?name=OpenShift"))
assert greeting["message"] == prefix + ", OpenShift!", greeting
info = json.loads(get("/api/info"))
assert info["javaVersion"] == 21
assert re.fullmatch("[a-f0-9]{40}", info["gitRef"]), info
if os.environ.get("EXPECTED_GIT_REF"):
    assert info["gitRef"] == os.environ["EXPECTED_GIT_REF"], info
for suffix in ("live", "ready", "started"):
    assert json.loads(get("/q/health/" + suffix))["status"] == "UP"
api = json.loads(get("/q/openapi"))
assert "/api/greetings" in api["paths"] and "/api/info" in api["paths"]
assert "box1_greetings_total" in get("/q/metrics", "text/plain")
try:
    get("/api/greetings?name=%20")
    raise AssertionError("Blank name should be rejected")
except urllib.error.HTTPError as error:
    assert error.code == 400, error.code
print("PASS: greeting, Java 21, health, OpenAPI, metrics, and validation at " + base)
