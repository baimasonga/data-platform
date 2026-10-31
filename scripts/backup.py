"""Encrypted, checksummed offline data-directory backup and safe restore."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import struct
import sys
import tarfile
import tempfile
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from app.locking import DataLock

MAGIC=b'DPB1'


def hash_file(path):
    digest=hashlib.sha256()
    with path.open('rb') as source:
        while block:=source.read(1024*1024): digest.update(block)
    return digest.hexdigest()


def key(path,create=False):
    path=Path(path)
    if create:
        descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(descriptor,'wb') as output: output.write(os.urandom(32))
    value=path.read_bytes()
    if len(value)!=32: raise ValueError('Backup key file must contain 32 bytes.')
    return value


def backup(base,destination,key_file):
    base,destination=Path(base).resolve(),Path(destination).resolve()
    if destination.is_relative_to(base): raise ValueError('Write backups outside the data directory.')
    if not base.is_dir(): raise ValueError('Data directory not found.')
    with DataLock(base),tempfile.TemporaryDirectory(prefix='platform-backup-') as temporary:
        archive=Path(temporary)/'data.tar.gz'
        manifest={}
        paths=[]
        for path in sorted(base.rglob('*')):
            if path.is_symlink(): raise ValueError('Data-directory symlinks are not supported by backup.')
            if path.is_file() and not path.name.endswith('.platform.lock'):
                relative=path.relative_to(base).as_posix()
                manifest[relative]=hash_file(path)
                paths.append((relative,path))
        with tarfile.open(archive,'w:gz') as output:
            payload=json.dumps({'format':1,'files':manifest},sort_keys=True).encode()
            import io
            info=tarfile.TarInfo('manifest.json');info.size=len(payload);info.mode=0o600
            output.addfile(info,io.BytesIO(payload))
            for relative,path in paths: output.add(path,arcname='files/'+relative,recursive=False)
        nonce=os.urandom(12)
        encryptor=Cipher(algorithms.AES(key(key_file)),modes.GCM(nonce)).encryptor()
        encryptor.authenticate_additional_data(MAGIC)
        descriptor=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        try:
            with os.fdopen(descriptor,'wb') as target,archive.open('rb') as source:
                target.write(MAGIC+nonce)
                while block:=source.read(1024*1024): target.write(encryptor.update(block))
                target.write(encryptor.finalize());target.write(encryptor.tag);target.flush();os.fsync(target.fileno())
        except Exception:
            destination.unlink(missing_ok=True)
            raise
    return {'files':len(manifest),'encrypted':True}


def restore(archive,destination,key_file):
    archive,destination=Path(archive).resolve(),Path(destination).resolve()
    with DataLock(destination),tempfile.TemporaryDirectory(prefix='platform-restore-') as temporary:
        if destination.exists() and any(destination.iterdir()): raise ValueError('Restore requires an empty destination; existing data will not be overwritten.')
        decrypted=Path(temporary)/'data.tar.gz'
        size=archive.stat().st_size
        if size<32: raise ValueError('Invalid encrypted backup.')
        with archive.open('rb') as source,decrypted.open('wb') as output:
            if source.read(4)!=MAGIC: raise ValueError('Unsupported backup format.')
            nonce=source.read(12);source.seek(-16,2);tag=source.read(16);source.seek(16)
            decryptor=Cipher(algorithms.AES(key(key_file)),modes.GCM(nonce,tag)).decryptor()
            decryptor.authenticate_additional_data(MAGIC)
            remaining=size-32
            while remaining:
                block=source.read(min(1024*1024,remaining));remaining-=len(block)
                if not block: raise ValueError('Truncated backup.')
                output.write(decryptor.update(block))
            output.write(decryptor.finalize())  # Authentication completes before extraction.
        staging=destination.parent/('.'+destination.name+'.restore-'+uuid.uuid4().hex)
        staging.mkdir(mode=0o700)
        try:
            with tarfile.open(decrypted,'r:gz') as source:
                members=source.getmembers()
                if len({member.name for member in members})!=len(members): raise ValueError('Duplicate archive members.')
                for member in members:
                    path=PurePosixPath(member.name)
                    if path.is_absolute() or '..' in path.parts or not member.isfile(): raise ValueError('Unsafe archive member.')
                manifest=json.load(source.extractfile('manifest.json'))
                if manifest.get('format')!=1: raise ValueError('Unsupported manifest format.')
                expected={'files/'+name for name in manifest['files']}|{'manifest.json'}
                if {member.name for member in members}!=expected: raise ValueError('Backup manifest does not match archive files.')
                for relative,checksum in manifest['files'].items():
                    path=PurePosixPath(relative)
                    if path.is_absolute() or '..' in path.parts: raise ValueError('Unsafe manifest path.')
                    target=staging/relative;target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
                    descriptor=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                    with os.fdopen(descriptor,'wb') as output,source.extractfile('files/'+relative) as content: shutil.copyfileobj(content,output)
                    if hash_file(target)!=checksum: raise ValueError('Restored checksum mismatch.')
            security=staging/'security.sqlite3'
            if security.exists():
                with sqlite3.connect(security) as con:
                    tables={row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    for table in ('sessions','states','api_tokens'):
                        if table in tables: con.execute('DELETE FROM '+table)
            if os.name=='nt' and destination.exists(): destination.rmdir()
            os.rename(staging,destination)
        except Exception:
            shutil.rmtree(staging,ignore_errors=True)
            raise
    return {'restored':True,'sessions_invalidated':True}


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('action',choices=['init-key','backup','restore'])
    parser.add_argument('--key-file',required=True)
    parser.add_argument('--data-dir')
    parser.add_argument('--archive')
    args=parser.parse_args()
    if args.action=='init-key': key(args.key_file,create=True);print('Backup key created; keep it separate from backups.')
    elif not args.data_dir or not args.archive: parser.error('--data-dir and --archive are required')
    elif args.action=='backup': print(json.dumps(backup(args.data_dir,args.archive,args.key_file)))
    else: print(json.dumps(restore(args.archive,args.data_dir,args.key_file)))
