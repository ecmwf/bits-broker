#!/usr/bin/env python3
"""Demonstrates creating and registering a custom TargetAction.

Run with:
    python bits-py/examples/custom_target.py
"""

import asyncio
from dataclasses import dataclass
from bits_py import TargetAction, Success, register_action


class Echo(TargetAction):
    """Return the request as a JSON body."""

    def __init__(self, label: str = "echo"):
        self.label = label

    async def dispatch(self, job):
        return Success.json({"route": self.label, "id": job.id, "request": job.request})


register_action("custom_echo_target", Echo)


@dataclass
class DemoJob:
    id: str
    request: dict


async def main():
    target = Echo(label="main")
    result = await target.dispatch(DemoJob(id="job-1", request={"type": "fc"}))

    print(f"created: {target}")
    print(f"dispatch result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
