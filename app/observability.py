"""Per-workspace request IDs and aggregate metrics; never log SQL or credentials."""
import logging
import threading
import time
import uuid


class Metrics:
    def __init__(self):
        self.guard=threading.Lock()
        self.requests=0
        self.errors=0
        self.active=0
        self.elapsed_ms=0.0

    def snapshot(self):
        with self.guard:
            return {'requests':self.requests,'errors':self.errors,'active':self.active,
                    'total_duration_ms':round(self.elapsed_ms,1)}


class RequestMetrics:
    def __init__(self, app, metrics):
        self.app,self.metrics=app,metrics

    async def __call__(self, scope, receive, send):
        if scope['type']!='http':
            return await self.app(scope,receive,send)
        ident=uuid.uuid4().hex
        start=time.monotonic()
        status=500
        with self.metrics.guard:
            self.metrics.active+=1
        async def capture(message):
            nonlocal status
            if message['type']=='http.response.start':
                status=message['status']
                message={**message,'headers':list(message.get('headers',[]))+[(b'x-request-id',ident.encode())]}
            await send(message)
        try:
            await self.app(scope,receive,capture)
        finally:
            elapsed=(time.monotonic()-start)*1000
            with self.metrics.guard:
                self.metrics.requests+=1
                self.metrics.errors+=int(status>=400)
                self.metrics.active-=1
                self.metrics.elapsed_ms+=elapsed
            if status>=500:
                logging.getLogger('platform.requests').error('request_id=%s method=%s status=%s duration_ms=%.1f',ident,scope['method'],status,elapsed)
