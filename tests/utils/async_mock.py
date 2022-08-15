#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest.mock import MagicMock
from pytest_mock import MockFixture


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


def async_stub(instance, name=None):
    return AsyncMock(spec=lambda *args, **kwargs: None, name=name)


def setup_async_stub():
    setattr(MockFixture, "async_stub", async_stub)
