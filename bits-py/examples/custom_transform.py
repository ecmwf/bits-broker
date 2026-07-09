#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""Demonstrates creating and registering a custom TransformAction.

Run with:
    python bits-py/examples/custom_transform.py
"""

import asyncio
from dataclasses import dataclass
from bits_py import TransformAction, Continue, register_action


class AddDefaults(TransformAction):
    """Merge default values into the request (existing keys are kept)."""

    def __init__(self, **defaults):
        self.defaults = defaults

    async def execute(self, job):
        for key, value in self.defaults.items():
            job.request.setdefault(key, value)
        return Continue()


register_action("custom_add_defaults", AddDefaults)


@dataclass
class DemoJob:
    request: dict


async def main():
    transform = AddDefaults(levtype="sfc", step="0")
    job = DemoJob(request={"type": "fc"})
    result = await transform.execute(job)

    print(f"created: {transform}")
    print(f"execute result: {result}")
    print(f"mutated request: {job.request}")


if __name__ == "__main__":
    asyncio.run(main())
