from typing import Any, List, Optional, Union

from pydantic.main import BaseModel

from apps.core import errors


class GeneralResponse(BaseModel):
    code: int
    data: Optional[Union[Any, List[Any]]]
    msg: Optional[str]


def get_error_response(error_code: int):
    return GeneralResponse(code=error_code, msg=errors.get(error_code))
