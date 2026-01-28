#!/usr/bin/env python3
import os
import errno
import shutil
from fuse import FUSE, FuseOSError, Operations
from pathlib import Path


class FuseCacheMtime(Operations):
    def __init__(self, source, cache_dir):
        self.source = source  # the sshfs mount
        self.cache_dir = cache_dir
        self.dir_mtimes = {}  # dir -> cached mtime
        self.file_mtimes = {}  # file -> cached mtime

    def _source_path(self, path):
        return os.path.join(self.source, path.lstrip('/'))

    def _cache_path(self, path):
        return os.path.join(self.cache_dir, path.lstrip('/'))

    def _needs_refresh(self, path):
        dir_path = os.path.dirname(path)
        source_dir = self._source_path(dir_path)
        
        try:
            current_mtime = os.stat(source_dir).st_mtime
        except OSError:
            return True
        
        cached_mtime = self.dir_mtimes.get(dir_path)
        return cached_mtime is None or current_mtime != cached_mtime

    def _refresh_dir(self, dir_path, priority_file=None):
        source_dir = self._source_path(dir_path)
        cache_dir = self._cache_path(dir_path)
        
        # ensure cache dir exists
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        
        # get current dir mtime
        dir_stat = os.stat(source_dir)
        
        # list all files
        try:
            entries = os.listdir(source_dir)
        except OSError as e:
            raise FuseOSError(e.errno)
        
        # fetch priority file first if specified
        if priority_file:
            filename = os.path.basename(priority_file)
            if filename in entries:
                self._fetch_file(priority_file)
                entries.remove(filename)
        
        # fetch remaining files (low priority / background)
        for entry in entries:
            file_path = os.path.join(dir_path, entry)
            source_file = self._source_path(file_path)
            if os.path.isfile(source_file):
                self._fetch_file(file_path)
        
        # update cached dir mtime
        self.dir_mtimes[dir_path] = dir_stat.st_mtime

    def _fetch_file(self, path):
        source_file = self._source_path(path)
        cache_file = self._cache_path(path)
        
        # check file mtime
        try:
            source_stat = os.stat(source_file)
        except OSError as e:
            raise FuseOSError(e.errno)
        
        cached_mtime = self.file_mtimes.get(path)
        
        if cached_mtime is None or source_stat.st_mtime != cached_mtime:
            # need to fetch
            Path(os.path.dirname(cache_file)).mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, cache_file)
            self.file_mtimes[path] = source_stat.st_mtime

    # --- Read operations (cached) ---

    def read(self, path, size, offset, fh):
        if self._needs_refresh(path):
            self._refresh_dir(os.path.dirname(path), priority_file=path)
        
        cache_file = self._cache_path(path)
        with open(cache_file, 'rb') as f:
            f.seek(offset)
            return f.read(size)

    def readdir(self, path, fh):
        source_dir = self._source_path(path)
        entries = ['.', '..']
        try:
            entries.extend(os.listdir(source_dir))
        except OSError as e:
            raise FuseOSError(e.errno)
        return entries

    def getattr(self, path, fh=None):
        # for attrs, check cache first, fall back to source
        cache_file = self._cache_path(path)
        source_file = self._source_path(path)
        
        if os.path.exists(cache_file):
            st = os.lstat(cache_file)
        else:
            try:
                st = os.lstat(source_file)
            except OSError as e:
                raise FuseOSError(e.errno)
        
        return dict((key, getattr(st, key)) for key in (
            'st_atime', 'st_ctime', 'st_gid', 'st_mode',
            'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    # --- Write operations (pass-through to source) ---

    def write(self, path, data, offset, fh):
        source_file = self._source_path(path)
        with open(source_file, 'r+b') as f:
            f.seek(offset)
            f.write(data)
        
        # invalidate cache for this file and dir
        dir_path = os.path.dirname(path)
        self.file_mtimes.pop(path, None)
        self.dir_mtimes.pop(dir_path, None)
        
        # also update local cache so reads are consistent
        cache_file = self._cache_path(path)
        if os.path.exists(cache_file):
            with open(cache_file, 'r+b') as f:
                f.seek(offset)
                f.write(data)
        
        return len(data)

    def create(self, path, mode, fi=None):
        source_file = self._source_path(path)
        fd = os.open(source_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        os.close(fd)
        
        # invalidate dir cache
        dir_path = os.path.dirname(path)
        self.dir_mtimes.pop(dir_path, None)
        
        return 0

    def truncate(self, path, length, fh=None):
        source_file = self._source_path(path)
        with open(source_file, 'r+b') as f:
            f.truncate(length)
        
        # invalidate caches
        dir_path = os.path.dirname(path)
        self.file_mtimes.pop(path, None)
        self.dir_mtimes.pop(dir_path, None)
        
        cache_file = self._cache_path(path)
        if os.path.exists(cache_file):
            with open(cache_file, 'r+b') as f:
                f.truncate(length)

    def unlink(self, path):
        source_file = self._source_path(path)
        os.unlink(source_file)
        
        # invalidate and remove from cache
        dir_path = os.path.dirname(path)
        self.file_mtimes.pop(path, None)
        self.dir_mtimes.pop(dir_path, None)
        
        cache_file = self._cache_path(path)
        if os.path.exists(cache_file):
            os.unlink(cache_file)

    def mkdir(self, path, mode):
        source_dir = self._source_path(path)
        os.mkdir(source_dir, mode)
        
        parent = os.path.dirname(path)
        self.dir_mtimes.pop(parent, None)

    def rmdir(self, path):
        source_dir = self._source_path(path)
        os.rmdir(source_dir)
        
        parent = os.path.dirname(path)
        self.dir_mtimes.pop(parent, None)
        
        cache_dir = self._cache_path(path)
        if os.path.exists(cache_dir):
            os.rmdir(cache_dir)

    def rename(self, old, new):
        source_old = self._source_path(old)
        source_new = self._source_path(new)
        os.rename(source_old, source_new)
        
        # invalidate both dirs
        self.dir_mtimes.pop(os.path.dirname(old), None)
        self.dir_mtimes.pop(os.path.dirname(new), None)
        self.file_mtimes.pop(old, None)
        
        # update cache
        cache_old = self._cache_path(old)
        cache_new = self._cache_path(new)
        if os.path.exists(cache_old):
            Path(os.path.dirname(cache_new)).mkdir(parents=True, exist_ok=True)
            os.rename(cache_old, cache_new)

    def chmod(self, path, mode):
        source_file = self._source_path(path)
        os.chmod(source_file, mode)

    def chown(self, path, uid, gid):
        source_file = self._source_path(path)
        os.chown(source_file, uid, gid)

    def utimens(self, path, times=None):
        source_file = self._source_path(path)
        os.utime(source_file, times)
        
        # invalidate since mtime changed
        self.file_mtimes.pop(path, None)
        self.dir_mtimes.pop(os.path.dirname(path), None)

    # --- File handle operations ---

    def open(self, path, flags):
        # we don't use real file handles, just return 0
        return 0

    def flush(self, path, fh):
        pass

    def release(self, path, fh):
        pass

    def fsync(self, path, datasync, fh):
        pass


def main():
    import argparse
    import tempfile
    import atexit

    parser = argparse.ArgumentParser(description='FUSE filesystem with mtime-based caching')
    parser.add_argument('source', help='Source directory (e.g., sshfs mount)')
    parser.add_argument('mountpoint', help='Where to mount the cached filesystem')
    parser.add_argument('--cache-dir', default=None,
                        help='Directory to store cached files (default: auto-created temp dir)')
    parser.add_argument('-o', dest='mount_options', default=None,
                        help='Mount options (accepted for fstab compatibility)')
    args = parser.parse_args()

    # Strip fuse-cache-mtime# prefix if present (from fstab)
    source = args.source
    if source.startswith('fuse-cache-mtime#'):
        source = source[len('fuse-cache-mtime#'):]

    # Parse mount options
    allow_other = False
    if args.mount_options:
        opts = args.mount_options.split(',')
        allow_other = 'allow_other' in opts

    # create temp dir if not specified
    if args.cache_dir is None:
        args.cache_dir = tempfile.mkdtemp(prefix='fuse-cache-mtime-')
        # clean up on exit
        def cleanup():
            shutil.rmtree(args.cache_dir, ignore_errors=True)
        atexit.register(cleanup)

    print(f"Mounting {source} -> {args.mountpoint} (cache: {args.cache_dir})")

    fuse = FUSE(
        FuseCacheMtime(source, args.cache_dir),
        args.mountpoint,
        foreground=True,
        allow_other=allow_other,
        nothreads=True,  # single-threaded reactor
    )


if __name__ == '__main__':
    main()