import chardet
import magic
import lm_dataformat as lmd
import os
import random
import sys
import traceback
import time
import shutil
import csv
from multiprocessing import cpu_count, Pool
from tqdm import tqdm
import argparse
import subprocess
from itertools import repeat

# from subprocess import DEVNULL, STDOUT, Popen

mime = magic.Magic(mime=True)


class TimeoutError(Exception):
    pass


def split_into_chunks(l, n):
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]


def is_digit(x):
    return x in "1234567890"


def keep(x):
    num_digits = len(list(filter(is_digit, x)))
    num_newlines = len(list(filter(lambda x: x == '\n', x)))
    if num_digits / len(x) > 0.8:
        return False

    # avg line length
    if len(x) / (num_newlines + .001) > 200:
        return False

    return True


def get_content(f):
    type = None
    try:
        enc = 'utf-8'
        type = mime.from_file(f)
        if not type.startswith('text'):
            return
        with open(f, 'rb') as fromfh:
            buf = fromfh.read()

        buf = buf.decode('UTF-8')
        if not keep(buf):
            return

        return buf
    except UnicodeDecodeError:
        # bad encoding, try different encoding
        try:
            enc = None
            enc = chardet.detect(buf)
            if enc['encoding'] is None:
                return
            buf = buf.decode(enc['encoding'])
            if not keep(buf):
                return
            return buf
        except UnicodeDecodeError:
            return
        except:
            err = traceback.format_exc()
            if verbose:
                print(err)
            time.sleep(0.1)
            return
    except KeyboardInterrupt:
        sys.exit()
    except FileNotFoundError:
        # bad symlink
        import os.path
        if not os.path.islink(f):
            # something went horribly wrong!
            ...
    except:
        err = traceback.format_exc()
        if verbose:
            print(err)
        time.sleep(0.1)
        return


def timeout(func, args=(), kwargs={}, timeout_duration=600, default=None):
    import signal

    def handler(signum, frame):
        raise TimeoutError()

    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout_duration)
    try:
        result = func(*args, **kwargs)
    except TimeoutError:
        result = default
    finally:
        signal.alarm(0)

    return result


def _process_repo(repo_data, repodir):
    out = None
    name, stars, lang = repo_data
    meta = {'repo_name': name, 'stars': stars, 'repo_language': lang}
    try:
        for curdir, dirs, files in os.walk(repodir):
            bad_extensions = [
                'app',
                'bin',
                'bmp',
                'bz2',
                'class',
                'csv',
                'dat',
                'db',
                'dll',
                'dylib',
                'egg',
                'eot',
                'exe',
                'gif',
                'gitignore',
                'glif',
                'gradle',
                'gz',
                'ico',
                'jar',
                'jpeg',
                'jpg',
                'lo',
                'lock',
                'log',
                'mp3',
                'mp4',
                'nar',
                'o',
                'ogg',
                'otf',
                'p',
                'pdf',
                'png',
                'pickle',
                'pkl',
                'pyc',
                'pyd',
                'pyo',
                'rkt',
                'so',
                'ss',
                'svg',
                'tar',
                'tsv',
                'ttf',
                'war',
                'webm',
                'woff',
                'woff2',
                'xz',
                'zip',
                'zst'
            ]

            files = [curdir + '/' + f for f in files if '.git' not in f and f[
                0] is not '.' and 'LICENSE' not in f and 'node_modules' not in f and '.min.' not in f and f.split('.')[
                         -1] not in bad_extensions]

            filenames = [f.split("/")[-1] for f in files]
            extensions = []
            for f in files:
                try:
                    extensions.append(mime.from_file(f))
                except FileNotFoundError:
                    extensions.append("none")
            text_outputs = list(map(get_content, files))
            for i in range(len(files)):
                text = text_outputs[i]
                if text is not None:
                    meta['file_name'] = filenames[i]
                    meta['mime_type'] = extensions[i]
                    if out is None:
                        out = [[text, meta]]
                    else:
                        out.append([text, meta])
        shutil.rmtree(repodir, ignore_errors=True)
    except TimeoutError:
        print(f"Processing for {name} timed out")
    return out


def process_repo(repo_data, repodir, processing_timeout):
    return timeout(_process_repo, args=(repo_data, repodir), timeout_duration=processing_timeout)


def process_repo_list(repo_data, clone_timeout, processing_timeout):
    out = None
    try:
        name, stars, lang = repo_data
        repodir = f'./.tmp/{name.split("/")[-1]}'
        p = subprocess.Popen(
            f'GIT_TERMINAL_PROMPT=0 git clone --depth 1 --single-branch https://github.com/{name} {repodir}',
            shell=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        try:
            p.wait(clone_timeout)
        except subprocess.TimeoutExpired:
            print(f'Git clone timed out for {name}')
            p.kill()
        shutil.rmtree(f'{repodir}/.git', ignore_errors=True)
        out = process_repo(repo_data, repodir, processing_timeout=processing_timeout)
    except Exception:
        err = traceback.format_exc()
        if verbose:
            print(err)
    return out


def filter_by_stars(repo_data, n_stars):
    return [item for item in repo_data if int(item[1]) >= n_stars]


def process_args():
    parser = argparse.ArgumentParser(
        description='CLI for github downloader - A tool for scraping repos as text from github')
    parser.add_argument('--n_threads', help='number of threads for parallel processing, defaults to cpu_count',
                        default=-1,
                        type=int)
    parser.add_argument('--n_stars', help='filter repos with less than n_stars stars',
                        default=-1,
                        type=int)
    parser.add_argument('--chunk_size', help='size of chunks to feed into each thread',
                        default=-1,
                        type=int)
    parser.add_argument('--clone_timeout', help='timeout for git clone command in seconds',
                        default=150,
                        type=int)
    parser.add_argument('--processing_timeout', help='timeout for processing repo to text files in seconds',
                        default=150,
                        type=int)
    parser.add_argument('-v', '--verbose', help='if flag is present, print errors', action='store_true')
    return parser.parse_args()


if __name__ == '__main__':

    args = process_args()  # parse args
    verbose = args.verbose

    # make output dirs
    if '.tmp' not in os.listdir():
        os.makedirs('.tmp')
    if 'github_data' not in os.listdir():
        os.makedirs('github_data')

    # read repo data to a tuple (reponame, n_stars, language)
    with open('github_repositories.csv', 'r') as f:
        csv_reader = csv.reader(f)
        repo_data = list(map(tuple, csv_reader))

    # filter by number of stars
    if args.n_stars != -1:
        repo_data = filter_by_stars(repo_data, args.n_stars)
    repo_data.sort()

    random.seed(420)
    random.shuffle(repo_data)

    n_threads = cpu_count() * 3 if args.n_threads == -1 else args.n_threads
    chunk_size = n_threads if args.chunk_size == -1 else args.chunk_size

    assert n_threads != 0

    # do work
    repo_chunks = split_into_chunks(repo_data, chunk_size)
    archive_name = 'github_data'
    ar = lmd.Archive(archive_name)
    pool = Pool(n_threads)
    pbar = tqdm(repo_chunks, total=len(repo_chunks))
    commit_every = 10
    success_hist = []
    for count, chunk in enumerate(pbar):
        repos_out = pool.starmap(process_repo_list,
                                 zip(chunk, repeat(args.clone_timeout), repeat(args.processing_timeout)))
        not_none = 0
        none = 0
        for repo in repos_out:
            if repo is not None:
                not_none += 1
                for f in repo:
                    print(f)
                    ar.add_data(f[0], meta=f[1])
            else:
                none += 1
        subprocess.Popen("rm -rfv .tmp && mkdir .tmp", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        if count % commit_every == 0:
            ar.commit()
        success_hist.append((not_none / len(repos_out)) * 100)
        success_rate = sum(success_hist) / len(success_hist)
        pbar.set_postfix({"Success Rate": success_rate})
