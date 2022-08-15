#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Optional
from fastapi import HTTPException


class XYZException(Exception): pass


class WorkException(XYZException): pass


class AirflowException(XYZException): pass
class AirflowAuthException(AirflowException): pass
class AirflowForbiddenException(AirflowException): pass
class AirflowUnknowException(AirflowException): pass


class InternalServiceException(XYZException):
    message = ""
    url = None
    method = None
    code = None

    def __init__(
        self,
        message: str = "",
        method: str = "GET",
        url: str = "",
        code: Optional[int] = None,
        exception: Optional[Exception] = None
    ) -> None:
        if message:
            self.message = message
        self._exception = exception
        self.code = code
        self.url = url
        super().__init__(message)

    @property
    def exception(self):
        return self._exception


class UserManagementServiceException(InternalServiceException): pass
class ResourceManagementServiceException(InternalServiceException): pass

class NotFoundException(XYZException, HTTPException): pass

