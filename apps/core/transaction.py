#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib


@contextlib.contextmanager
def transaction(session):
    if not session.in_transaction():
        with session.begin():
            yield
    else:
        with session.begin_nested():
            yield
