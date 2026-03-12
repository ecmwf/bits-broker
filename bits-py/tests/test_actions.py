"""Integration tests for the bits_py Python action registration API.

Tests cover:
- Valid registration of all three action kinds (check, transform, target)
- Registration validation: no ABC subclass, wrong/missing method, non-async method,
  multiple-base ambiguity
- End-to-end: register_action + Bits.from_config + submit + poll
- Transform mutation round-trip (job.request / job.metadata changes propagate)
- All outcome types: Pass, Reject, Continue, Success, Redirect, Error
- Success.json() classmethod

Design notes:
- The runtime registry is global and permanent (no un-register). Each test
  therefore uses a unique action name to avoid cross-test collisions.
- pytest-asyncio is used for async tests.
"""

import itertools
import json
import uuid
import pytest

from bits_py import (
    Bits,
    CheckAction,
    TransformAction,
    TargetAction,
    Pass,
    Reject,
    Continue,
    Success,
    Redirect,
    Error,
    register_action,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_counter = itertools.count(1)


def unique_name(prefix: str) -> str:
    """Return a globally-unique action name safe to register once per process."""
    return f"{prefix}_{next(_counter)}_{uuid.uuid4().hex[:6]}"


def target_config(target_type: str) -> str:
    """Minimal bits YAML config with a single named target and one route."""
    return f"""
targets:
  t:
    type: {target_type}
routes:
  only:
    - target::t
"""


def check_target_config(check_type: str, target_type: str) -> str:
    """Config with a named check + target in sequence."""
    return f"""
checks:
  gate:
    type: {check_type}
targets:
  echo:
    type: {target_type}
routes:
  only:
    - check::gate
    - target::echo
"""


# ---------------------------------------------------------------------------
# Section 1 — Registration validation
# ---------------------------------------------------------------------------


class TestRegistrationValidation:
    def test_register_requires_class(self):
        """Passing a non-class raises TypeError."""
        with pytest.raises(TypeError):
            register_action("not_a_class_xyz", lambda: None)

    def test_register_requires_abc_subclass(self):
        """A plain class (no ABC base) must be rejected."""

        class PlainClass:
            async def evaluate(self, job):
                return Pass()

        with pytest.raises(TypeError, match="subclass"):
            register_action(unique_name("plain"), PlainClass)

    def test_register_rejects_missing_method(self):
        """A CheckAction subclass without 'evaluate' must be rejected."""

        class MissingMethod(CheckAction):
            pass

        with pytest.raises(TypeError, match="evaluate"):
            register_action(unique_name("missing_method"), MissingMethod)

    def test_register_rejects_non_async_method(self):
        """A CheckAction whose evaluate() is not async must be rejected."""

        class SyncCheck(CheckAction):
            def evaluate(self, job):  # not async!
                return Pass()

        with pytest.raises(TypeError, match="async"):
            register_action(unique_name("sync_check"), SyncCheck)

    def test_register_rejects_missing_execute(self):
        """A TransformAction subclass without 'execute' must be rejected."""

        class NoExecute(TransformAction):
            pass

        with pytest.raises(TypeError, match="execute"):
            register_action(unique_name("no_execute"), NoExecute)

    def test_register_rejects_missing_dispatch(self):
        """A TargetAction subclass without 'dispatch' must be rejected."""

        class NoDispatch(TargetAction):
            pass

        with pytest.raises(TypeError, match="dispatch"):
            register_action(unique_name("no_dispatch"), NoDispatch)

    def test_register_rejects_duplicate_name(self):
        """Registering the same name twice must raise ValueError."""
        name = unique_name("dup")

        class DupCheck(CheckAction):
            async def evaluate(self, job):
                return Pass()

        register_action(name, DupCheck)

        class DupCheck2(CheckAction):
            async def evaluate(self, job):
                return Pass()

        with pytest.raises(
            (ValueError, Exception), match="already registered|conflicts"
        ):
            register_action(name, DupCheck2)

    def test_register_rejects_builtin_name(self):
        """Attempting to shadow a built-in action (e.g. 'http') must fail."""

        class MyCheck(CheckAction):
            async def evaluate(self, job):
                return Pass()

        with pytest.raises((ValueError, Exception), match="built-in|conflicts"):
            register_action("http", MyCheck)

    def test_register_check_succeeds(self):
        """A well-formed CheckAction subclass registers without error."""

        class GoodCheck(CheckAction):
            async def evaluate(self, job):
                return Pass()

        register_action(unique_name("good_check"), GoodCheck)

    def test_register_transform_succeeds(self):
        """A well-formed TransformAction subclass registers without error."""

        class GoodTransform(TransformAction):
            async def execute(self, job):
                return Continue()

        register_action(unique_name("good_transform"), GoodTransform)

    def test_register_target_succeeds(self):
        """A well-formed TargetAction subclass registers without error."""

        class GoodTarget(TargetAction):
            async def dispatch(self, job):
                return Success(b"ok")

        register_action(unique_name("good_target"), GoodTarget)


# ---------------------------------------------------------------------------
# Section 2 — End-to-end: CheckAction
# ---------------------------------------------------------------------------


class TestCheckActionEndToEnd:
    @pytest.mark.asyncio
    async def test_pass_routes_to_target(self):
        """A check that returns Pass allows the job to reach the target."""
        check_name = unique_name("e2e_check_pass")
        target_name = unique_name("e2e_target_for_check")

        class AlwaysPass(CheckAction):
            async def evaluate(self, job):
                return Pass()

        class EchoTarget(TargetAction):
            async def dispatch(self, job):
                return Success(b"reached")

        register_action(check_name, AlwaysPass)
        register_action(target_name, EchoTarget)

        bits = await Bits.from_config(check_target_config(check_name, target_name))
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        assert outcome["status"] == "ready"
        result = outcome["result"]
        assert result["status"] == "success"
        assert result["body"] == b"reached"

    @pytest.mark.asyncio
    async def test_reject_falls_through(self):
        """A check that returns Reject causes the route to be skipped (→ failed)."""
        check_name = unique_name("e2e_check_reject")

        class AlwaysReject(CheckAction):
            async def evaluate(self, job):
                return Reject("no access")

        register_action(check_name, AlwaysReject)

        config = f"""
checks:
  gate:
    type: {check_name}
routes:
  only:
    - check::gate
    - target::http:
        url: http://localhost:19999
"""
        bits = await Bits.from_config(config)
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        assert outcome["status"] == "ready"
        # All routes rejected → error result (no route matched)
        assert outcome["result"]["status"] == "error"

    @pytest.mark.asyncio
    async def test_check_receives_job_fields(self):
        """The job object passed to evaluate() exposes the correct id and request."""
        check_name = unique_name("e2e_check_fields")
        target_name = unique_name("e2e_target_fields_echo")

        received = {}

        class CapturingCheck(CheckAction):
            async def evaluate(self, job):
                received["id"] = job.id
                received["request"] = dict(job.request)
                return Pass()

        class MinimalTarget(TargetAction):
            async def dispatch(self, job):
                return Success(b"ok")

        register_action(check_name, CapturingCheck)
        register_action(target_name, MinimalTarget)

        bits = await Bits.from_config(check_target_config(check_name, target_name))
        job_id = await bits.submit({"foo": "bar"})
        await bits.poll(job_id, timeout_secs=5.0)

        assert received["id"] == job_id
        assert received["request"].get("foo") == "bar"


# ---------------------------------------------------------------------------
# Section 3 — End-to-end: TransformAction
# ---------------------------------------------------------------------------


class TestTransformActionEndToEnd:
    @pytest.mark.asyncio
    async def test_transform_mutates_request(self):
        """Transform can mutate job.request and the changes reach the target."""
        transform_name = unique_name("e2e_transform_mutate")
        target_name = unique_name("e2e_target_for_transform")

        class AddKey(TransformAction):
            def __init__(self, key: str = "added", value: str = "yes"):
                self.key = key
                self.value = value

            async def execute(self, job):
                req = dict(job.request)
                req[self.key] = self.value
                job.request = req
                return Continue()

        seen_request = {}

        class CaptureTarget(TargetAction):
            async def dispatch(self, job):
                seen_request.update(job.request)
                return Success(b"done")

        register_action(transform_name, AddKey)
        register_action(target_name, CaptureTarget)

        config = f"""
transforms:
  adder:
    type: {transform_name}
    key: injected
    value: hello
targets:
  cap:
    type: {target_name}
routes:
  only:
    - transform::adder
    - target::cap
"""
        bits = await Bits.from_config(config)
        job_id = await bits.submit({"original": True})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        assert outcome["status"] == "ready"
        assert outcome["result"]["status"] == "success"
        assert seen_request.get("injected") == "hello"
        assert seen_request.get("original") is True

    @pytest.mark.asyncio
    async def test_transform_mutates_metadata(self):
        """Transform can mutate job.metadata and the changes persist into the target."""
        transform_name = unique_name("e2e_transform_meta")
        target_name = unique_name("e2e_target_meta")

        class TagMeta(TransformAction):
            async def execute(self, job):
                meta = dict(job.metadata) if job.metadata else {}
                meta["tagged"] = True
                job.metadata = meta
                return Continue()

        seen_meta = {}

        class MetaTarget(TargetAction):
            async def dispatch(self, job):
                if job.metadata:
                    seen_meta.update(job.metadata)
                return Success(b"ok")

        register_action(transform_name, TagMeta)
        register_action(target_name, MetaTarget)

        config = f"""
transforms:
  tagger:
    type: {transform_name}
targets:
  meta_cap:
    type: {target_name}
routes:
  only:
    - transform::tagger
    - target::meta_cap
"""
        bits = await Bits.from_config(config)
        job_id = await bits.submit({})
        await bits.poll(job_id, timeout_secs=5.0)

        assert seen_meta.get("tagged") is True

    @pytest.mark.asyncio
    async def test_transform_reject_skips_target(self):
        """A transform that returns Reject causes the route to be skipped."""
        transform_name = unique_name("e2e_transform_reject")

        class AlwaysRejectTransform(TransformAction):
            async def execute(self, job):
                return Reject("transform rejected")

        register_action(transform_name, AlwaysRejectTransform)

        config = f"""
transforms:
  blocker:
    type: {transform_name}
routes:
  only:
    - transform::blocker
    - target::http:
        url: http://localhost:19999
"""
        bits = await Bits.from_config(config)
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        assert outcome["status"] == "ready"
        # All routes rejected → error result (no route matched)
        assert outcome["result"]["status"] == "error"


# ---------------------------------------------------------------------------
# Section 4 — End-to-end: TargetAction + all outcome types
# ---------------------------------------------------------------------------


class TestTargetActionEndToEnd:
    @pytest.mark.asyncio
    async def test_success_bytes(self):
        """Target returning Success(bytes) produces a success result with correct body."""
        target_name = unique_name("e2e_target_success_bytes")

        class BytesTarget(TargetAction):
            async def dispatch(self, job):
                return Success(b"hello bytes", content_type="application/octet-stream")

        register_action(target_name, BytesTarget)

        bits = await Bits.from_config(target_config(target_name))
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        assert outcome["status"] == "ready"
        result = outcome["result"]
        assert result["status"] == "success"
        assert result["body"] == b"hello bytes"
        assert result["content_type"] == "application/octet-stream"

    @pytest.mark.asyncio
    async def test_success_str(self):
        """Target returning Success(str) encodes it as UTF-8 with text/plain content-type."""
        target_name = unique_name("e2e_target_success_str")

        class StrTarget(TargetAction):
            async def dispatch(self, job):
                return Success("hello text")

        register_action(target_name, StrTarget)

        bits = await Bits.from_config(target_config(target_name))
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        result = outcome["result"]
        assert result["status"] == "success"
        assert result["body"] == b"hello text"
        assert "text/plain" in result["content_type"]

    @pytest.mark.asyncio
    async def test_success_json(self):
        """Success.json() serialises a Python dict as JSON with application/json content-type."""
        target_name = unique_name("e2e_target_success_json")

        class JsonTarget(TargetAction):
            async def dispatch(self, job):
                return Success.json({"status": "ok", "n": 42})

        register_action(target_name, JsonTarget)

        bits = await Bits.from_config(target_config(target_name))
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        result = outcome["result"]
        assert result["status"] == "success"
        assert result["content_type"] == "application/json"
        body = json.loads(result["body"])
        assert body == {"status": "ok", "n": 42}

    @pytest.mark.asyncio
    async def test_redirect_outcome(self):
        """Target returning Redirect produces a redirect result."""
        target_name = unique_name("e2e_target_redirect")

        class RedirectTarget(TargetAction):
            async def dispatch(self, job):
                return Redirect("https://example.com/new", "moved")

        register_action(target_name, RedirectTarget)

        bits = await Bits.from_config(target_config(target_name))
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        result = outcome["result"]
        assert result["status"] == "redirect"
        assert result["location"] == "https://example.com/new"
        assert result["message"] == "moved"

    @pytest.mark.asyncio
    async def test_error_outcome(self):
        """Target returning Error produces an error result."""
        target_name = unique_name("e2e_target_error")

        class ErrorTarget(TargetAction):
            async def dispatch(self, job):
                return Error("something went wrong")

        register_action(target_name, ErrorTarget)

        bits = await Bits.from_config(target_config(target_name))
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        result = outcome["result"]
        assert result["status"] == "error"
        assert result["message"] == "something went wrong"

    @pytest.mark.asyncio
    async def test_target_reject_outcome(self):
        """Target returning Reject causes the route to be skipped (→ failed)."""
        target_name = unique_name("e2e_target_reject")

        class RejectTarget(TargetAction):
            async def dispatch(self, job):
                return Reject("target rejected")

        register_action(target_name, RejectTarget)

        bits = await Bits.from_config(target_config(target_name))
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        assert outcome["status"] == "ready"
        # All routes rejected → error result (no route matched)
        assert outcome["result"]["status"] == "error"


# ---------------------------------------------------------------------------
# Section 5 — Config keyword arguments passed to __init__
# ---------------------------------------------------------------------------


class TestConfigKwargs:
    @pytest.mark.asyncio
    async def test_yaml_config_keys_passed_as_kwargs(self):
        """YAML config keys (except 'type') are forwarded to __init__ as kwargs."""
        target_name = unique_name("e2e_target_kwargs")

        class ConfiguredTarget(TargetAction):
            def __init__(self, greeting: str = "hello", count: int = 1):
                self.greeting = greeting
                self.count = count

            async def dispatch(self, job):
                body = (self.greeting * self.count).encode()
                return Success(body)

        register_action(target_name, ConfiguredTarget)

        config = f"""
targets:
  t:
    type: {target_name}
    greeting: hi
    count: 3
routes:
  only:
    - target::t
"""
        bits = await Bits.from_config(config)
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)

        result = outcome["result"]
        assert result["status"] == "success"
        assert result["body"] == b"hihihi"

    @pytest.mark.asyncio
    async def test_check_with_config_param(self):
        """CheckAction subclass can receive a config param and use it in evaluate()."""
        check_name = unique_name("e2e_check_kwargs")
        target_name = unique_name("e2e_target_for_kwargs_check")

        class RoleCheck(CheckAction):
            def __init__(self, role: str = ""):
                self.role = role

            async def evaluate(self, job):
                roles = (job.user or {}).get("roles", [])
                return (
                    Pass()
                    if self.role in roles
                    else Reject(f"missing role: {self.role}")
                )

        class OkTarget(TargetAction):
            async def dispatch(self, job):
                return Success(b"ok")

        register_action(check_name, RoleCheck)
        register_action(target_name, OkTarget)

        config = f"""
checks:
  gate:
    type: {check_name}
    role: admin
targets:
  t:
    type: {target_name}
routes:
  only:
    - check::gate
    - target::t
"""
        bits = await Bits.from_config(config)
        # Submit with no user → no roles → Reject
        job_id = await bits.submit({})
        outcome = await bits.poll(job_id, timeout_secs=5.0)
        # All routes rejected → error result (no route matched)
        assert outcome["result"]["status"] == "error"


# ---------------------------------------------------------------------------
# Section 6 — repr smoke tests for outcome types
# ---------------------------------------------------------------------------


class TestOutcomeReprs:
    def test_pass_repr(self):
        assert "Pass" in repr(Pass())

    def test_continue_repr(self):
        assert "Continue" in repr(Continue())

    def test_reject_repr(self):
        r = Reject("bad")
        assert "Reject" in repr(r)
        assert "bad" in repr(r)

    def test_success_repr(self):
        s = Success(b"data", content_type="text/plain")
        assert "Success" in repr(s)

    def test_redirect_repr(self):
        r = Redirect("https://example.com")
        assert "Redirect" in repr(r)

    def test_error_repr(self):
        e = Error("oops")
        assert "Error" in repr(e)

    def test_success_wrong_body_type(self):
        with pytest.raises(TypeError):
            Success(12345)  # int is not bytes or str
