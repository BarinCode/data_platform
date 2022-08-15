from jose import jwt
from loguru import logger
from fastapi import APIRouter
from fastapi.requests import Request
from fastapi.responses import Response, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint


from apps.config import settings

APIRouter.url_path_for

def authenticate(token):
    """
        token 校验
    """
    token = token.replace('Bearer ', '')
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=settings.jwt_algorithm
        )
        return payload
    except (jwt.JWTError, jwt.ExpiredSignatureError, AttributeError) as e:
        logger.info(e)
        return 


def permission(user, router):
    """
        权限校验
    """
    if router not in user.get('all', []):
        return True

    if router not in user.get('modules', []):
        return False
    return True


class AuthMiddleware(BaseHTTPMiddleware):

    async def auth(self, request:Request, router:str):
        token = (
            request.headers.get('authorization')
            or request.query_params._dict.get('token')
        )
        
        if token is None:
            return JSONResponse({'code':400401, "msg":'token invaild'}, status_code=403)
        
        user = authenticate(token)
        if user is None:
            return JSONResponse({'code':400401, "msg":'token invaild'}, status_code=403)

        is_allow = permission(user, router)
        if not is_allow:
            return JSONResponse({'code':400403, "msg":'user forbidden'}, status_code=403)


    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = None
        router = request.url.path.strip('/').split('/')
        if len(router) >= 2 and router[-1] != 'login':
            response = await self.auth(request, router)
        
        if response is None:
            response = await call_next(request)

        return response
