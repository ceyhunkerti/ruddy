from ruddy.server.middleware.core_middleware import (
    CoreMiddleWareFactory,
    CoreMiddleware,
)

CORE_MIDDLEWARE = "__core__"

middlewares = {
    CORE_MIDDLEWARE: CoreMiddleWareFactory(),
}
