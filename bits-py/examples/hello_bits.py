#!/usr/bin/env python3
"""Minimal bits_py example: register a custom target, submit a job, print the result.

Run with:
    python bits-py/examples/hello_bits.py
"""

import asyncio
from bits_py import Bits, TargetAction, Success, register_action


# A target that echoes the request back as JSON — no external server needed.
class Echo(TargetAction):
    def __init__(self, label: str = "echo"):
        self.label = label

    async def dispatch(self, job):
        return Success.json({"route": self.label, "request": job.request})


register_action("echo", Echo)

CONFIG = """
routes:
  default:
    - target::echo:
        label: hello
"""


async def main():
    bits = await Bits.from_config(CONFIG)

    job_id = await bits.submit({"greeting": "hello, bits!"})
    outcome = await bits.poll(job_id)

    result = outcome["result"]
    if result["status"] == "success":
        print(result["body"].decode())
    else:
        print(f"unexpected: {result}")


if __name__ == "__main__":
    asyncio.run(main())
