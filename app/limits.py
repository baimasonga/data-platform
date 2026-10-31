"""Bound request bodies before FastAPI's multipart parser receives them."""
from starlette.responses import JSONResponse


class RequestLimit:
    def __init__(self, app, limit):
        self.app, self.limit = app, limit

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or scope['method'] not in ('POST', 'PUT', 'PATCH'):
            return await self.app(scope, receive, send)
        upload=scope['path']=='/api/datasets' or (scope['path'].startswith('/api/datasets/') and scope['path'].endswith('/versions'))
        limit=self.limit if upload else min(self.limit,1024*1024)
        messages, size = [], 0
        while True:
            message = await receive()
            if message['type'] == 'http.disconnect':
                return
            size += len(message.get('body', b''))
            if size > limit:
                return await JSONResponse({'detail': 'Request body exceeds the size limit.'}, 413)(scope, receive, send)
            messages.append(message)
            if not message.get('more_body', False):
                break
        async def replay():
            if messages:
                return messages.pop(0)
            return await receive()
        await self.app(scope, replay, send)
