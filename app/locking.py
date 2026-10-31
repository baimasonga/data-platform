"""Single-worker data-directory leases shared with offline backup/restore tools."""
import os
if os.name=='nt':
    import msvcrt
else:
    import fcntl
from pathlib import Path
import threading


class DataLock:
    def __init__(self, base):
        self.base=Path(base).resolve()
        self.path=self.base.parent / ('.'+self.base.name+'.platform.lock')
        self.handle=None
        self.depth=0
        self.guard=threading.RLock()

    def acquire(self):
        with self.guard:
            if self.handle is not None:
                self.depth+=1
                return self
            self.path.parent.mkdir(parents=True,exist_ok=True)
            handle=self.path.open('a+b')
            try:
                if os.name=='nt':
                    if handle.seek(0,2)==0: handle.write(b'\0');handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
                else:
                    fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except (BlockingIOError,OSError):
                handle.close()
                raise RuntimeError('Data directory is in use. Stop the owning server before starting another worker or restoring data.') from None
            self.handle,self.depth=handle,1
            return self

    def close(self):
        with self.guard:
            if self.handle is not None:
                self.depth-=1
                if self.depth==0:
                    self.handle.close()
                    self.handle=None

    def __enter__(self):
        return self.acquire()

    def __exit__(self,*args):
        self.close()
