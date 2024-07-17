#! /usr/bin/env python

# Import necessary modules
import errno
import os
import subprocess as sp
import sys
import time
from dateutil import tz
from datetime import datetime

# Compatibility for Python 2 and 3 for quoting shell arguments
try:
    from shlex import quote
except ImportError:
    from pipes import quote

# Define public API and version
__all__ = ['gh_import_folder_as_branch']
__version__ = "0.1.0"


# Custom exception class for ghp_import errors
class GhiError(Exception):
    def __init__(self, message):
        self.message = message


# Encoding and decoding helpers for Python 2 and 3 compatibility
if sys.version_info[0] == 3:
    def enc(text):
        if isinstance(text, bytes):
            return text
        return text.encode()

    def dec(text):
        if isinstance(text, bytes):
            return text.decode('utf-8')
        return text

    def write(pipe, data):
        try:
            pipe.stdin.write(data)
        except IOError as e:
            if e.errno != errno.EPIPE:
                raise
else:
    def enc(text):
        if isinstance(text, unicode):  # noqa F821
            return text.encode('utf-8')
        return text

    def dec(text):
        if isinstance(text, unicode):  # noqa F821
            return text
        return text.decode('utf-8')

    def write(pipe, data):
        pipe.stdin.write(data)


# Git command wrapper class
class Git(object):
    def __init__(self, use_shell=False):
        self.use_shell = use_shell

        self.cmd = None
        self.pipe = None
        self.stderr = None
        self.stdout = None

    def check_repo(self):
        # Check if current directory is a Git repository
        if self.call('rev-parse') != 0:
            error = self.stderr
            if not error:
                error = "Unknown Git error"
            error = dec(error)
            if error.startswith("fatal: "):
                error = error[len("fatal: "):]
            raise GhiError(error)

    def try_rebase(self, remote, branch, no_history=False):
        # Attempt to rebase the specified branch
        rc = self.call('rev-list', '--max-count=1', '%s/%s' % (remote, branch))
        if rc != 0:
            return True
        rev = dec(self.stdout.strip())
        if no_history:
            rc = self.call('update-ref', '-d', 'refs/heads/%s' % branch)
        else:
            rc = self.call('update-ref', 'refs/heads/%s' % branch, rev)
        if rc != 0:
            return False
        return True

    def get_config(self, key):
        # Get Git configuration value
        self.call('config', key)
        return self.stdout.strip()

    def get_prev_commit(self, branch):
        # Get the previous commit ID for a branch
        rc = self.call('rev-list', '--max-count=1', branch, '--')
        if rc != 0:
            return None
        return dec(self.stdout).strip()

    def open(self, *args, **kwargs):
        # Open a subprocess for a Git command
        if self.use_shell:
            self.cmd = 'git ' + ' '.join(map(quote, args))
        else:
            self.cmd = ['git'] + list(args)
        if sys.version_info >= (3, 2, 0):
            kwargs['universal_newlines'] = False
        for k in 'stdin stdout stderr'.split():
            kwargs.setdefault(k, sp.PIPE)
        kwargs['shell'] = self.use_shell
        print(self.cmd)
        self.pipe = sp.Popen(self.cmd, **kwargs)
        return self.pipe

    def call(self, *args, **kwargs):
        # Call a Git command and return its exit status
        self.open(*args, **kwargs)
        (self.stdout, self.stderr) = self.pipe.communicate()
        return self.pipe.wait()

    def check_call(self, *args, **kwargs):
        # Call a Git command and check its exit status
        kwargs["shell"] = self.use_shell
        sp.check_call(['git'] + list(args), **kwargs)


# Helper function to create a timestamp for commits
def mk_when(timestamp=None):
    if timestamp is None:
        timestamp = int(time.time())
    currtz = datetime.now(tz.tzlocal()).strftime('%z')
    return "%s %s" % (timestamp, currtz)


# Start a new commit in the Git fast-import stream
def start_commit(pipe, git, branch, message, prefix=None):
    uname = os.getenv('GIT_COMMITTER_NAME', dec(git.get_config('user.name')))
    email = os.getenv('GIT_COMMITTER_EMAIL', dec(git.get_config('user.email')))
    when = os.getenv('GIT_COMMITTER_DATE', mk_when())
    write(pipe, enc('commit refs/heads/%s\n' % branch))
    write(pipe, enc('committer %s <%s> %s\n' % (uname, email, when)))
    write(pipe, enc('data %d\n%s\n' % (len(enc(message)), message)))
    head = git.get_prev_commit(branch)
    if head:
        write(pipe, enc('from %s\n' % head))
    if prefix:
        write(pipe, enc('D %s\n' % prefix))
    else:
        write(pipe, enc('deleteall\n'))


# Add a file to the Git fast-import stream
def add_file(pipe, srcpath, tgtpath):
    with open(srcpath, "rb") as handle:
        if os.access(srcpath, os.X_OK):
            write(pipe, enc('M 100755 inline %s\n' % tgtpath))
        else:
            write(pipe, enc('M 100644 inline %s\n' % tgtpath))
        data = handle.read()
        write(pipe, enc('data %d\n' % len(data)))
        write(pipe, enc(data))
        write(pipe, enc('\n'))


# Convert a file path to a Git-compatible path
def gitpath(fname):
    norm = os.path.normpath(fname)
    return "/".join(norm.split(os.path.sep))


# Main function to run the import process
def run_import(git, srcdir, **opts):
    srcdir = dec(srcdir)
    pipe = git.open('fast-import', '--date-format=rfc2822', '--quiet',
                    stdin=sp.PIPE, stdout=None, stderr=None)
    start_commit(pipe, git, opts['branch'], opts['mesg'], opts['prefix'])
    for path, _, fnames in os.walk(srcdir, followlinks=opts['followlinks']):
        for fn in fnames:
            fpath = os.path.join(path, fn)
            gpath = gitpath(os.path.relpath(fpath, start=srcdir))
            if opts['prefix']:
                gpath = os.path.join(opts['prefix'], gpath)
            add_file(pipe, fpath, gpath)   
    write(pipe, enc('\n'))
    pipe.stdin.close()
    if pipe.wait() != 0:
        sys.stdout.write(enc("Failed to process commit.\n"))


# Define command-line options for the script
def options():
    return [
        (('-m', '--message'), dict(
            dest='mesg',
            default='Update documentation',
            help='The commit message to use on the target branch.',
        )),
        (('-p', '--push'), dict(
            dest='push',
            default=False,
            action='store_true',
            help='Push the branch to origin/{branch} after committing.',
        )),
        (('-x', '--prefix'), dict(
            dest='prefix',
            default=None,
            help='The prefix to add to each file that gets pushed to the '
                    'remote. Only files below this prefix will be cleared '
                    'out. [%(default)s]',
        )),
        (('-f', '--force'), dict(
            dest='force',
            default=False, action='store_true',
            help='Force the push to the repository.',
        )),
        (('-o', '--no-history'), dict(
            dest='no_history',
            default=False,
            action='store_true',
            help='Force new commit without parent history.',
        )),
        (('-r', '--remote'), dict(
            dest='remote',
            default='origin',
            help='The name of the remote to push to. [%(default)s]',
        )),
        # Option for specifying the branch to push to
        (('-b', '--branch'), dict(
            dest='branch',
            default='gh-pages',
            help='Name of the branch to write to. [%(default)s]',
        )),
        # Option for using the shell when invoking Git
        (('-s', '--shell'), dict(
            dest='use_shell',
            default=False,
            action='store_true',
            help='Use the shell when invoking Git. [%(default)s]',
        )),
        # Option for following symlinks when adding files
        (('-l', '--follow-links'), dict(
            dest='followlinks',
            default=False,
            action='store_true',
            help='Follow symlinks when adding files. [%(default)s]',
        ))
    ]


# Function to import a directory to gh-pages
def gh_import_folder_as_branch(srcdir, **kwargs):
    # Check if the source directory exists
    if not os.path.isdir(srcdir):
        raise GhiError("Not a directory: %s" % srcdir)

    # Initialize options with defaults and update with any provided keyword arguments
    opts = {kwargs["dest"]: kwargs["default"] for _, kwargs in options()}
    opts.update(kwargs)

    # Initialize Git with options
    git = Git(use_shell=opts['use_shell'])
    git.check_repo()  # Check if the current directory is a git repository

    # Attempt to rebase the specified branch
    if not git.try_rebase(opts['remote'], opts['branch'], opts['no_history']):
        raise GhiError("Failed to rebase %s branch." % opts['branch'])

    # Run the import process
    run_import(git, srcdir, **opts)

    # Push changes to the remote if specified
    if opts['push']:
        if opts['force'] or opts['no_history']:
            git.check_call('push', opts['remote'], opts['branch'], '--force')
        else:
            git.check_call('push', opts['remote'], opts['branch'])


# Main function to parse arguments and call the import function
def main():
    from argparse import ArgumentParser

    # Initialize argument parser
    parser = ArgumentParser()
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("directory")
    # Add defined options to the parser
    for args, kwargs in options():
        parser.add_argument(*args, **kwargs)

    # Parse arguments
    args = parser.parse_args().__dict__

    # Attempt to import the directory, handle errors gracefully
    try:
        gh_import_folder_as_branch(args.pop("directory"), **args)
    except GhiError as e:
        parser.error(e.message)


# Entry point of the script
if __name__ == '__main__':
    main()