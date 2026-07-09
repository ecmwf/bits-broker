#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

"""Demonstrates creating and registering a custom CheckAction.

Run with:
    python bits-py/examples/custom_check.py
"""

import asyncio
from dataclasses import dataclass
from bits_py import CheckAction, Pass, Reject, register_action


class RequiredField(CheckAction):
    """Reject jobs that are missing a required request field."""

    def __init__(self, field: str):
        self.field = field

    async def evaluate(self, job):
        if self.field in job.request:
            return Pass()
        return Reject(f"missing required field: {self.field}")


register_action("custom_required_field", RequiredField)


@dataclass
class DemoJob:
    request: dict


async def main():
    check = RequiredField(field="date")

    passed = await check.evaluate(DemoJob(request={"type": "fc", "date": "2025-06-01"}))
    rejected = await check.evaluate(DemoJob(request={"type": "fc"}))

    print(f"created: {check}")
    print(f"pass result: {passed}")
    print(f"reject result: {rejected}")


if __name__ == "__main__":
    asyncio.run(main())
