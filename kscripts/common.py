import subprocess
import logging
import sys
from threading import Thread
from io import BytesIO


class ProcessException(Exception):
  def __init__(self, code, stdout, stderr):
    self.stdout = stdout
    self.stderr = stderr
    self.code = code

  def __repr__(self):
    return 'ProcessException(\ncode=%s\nstderr=\n%s\nstdout=\n%s\n)' % (
      self.code, self.stderr, self.stdout)


class TeeStream(Thread):
  def __init__(self, quiet, input_stream, output_stream=sys.stdout, prefix=''):
    Thread.__init__(self)
    self._output = BytesIO()
    self.output_stream = output_stream
    self.input_stream = input_stream
    self.quiet = quiet
    self.prefix = prefix

  def run(self):
    while 1:
      line = self.input_stream.read()
      if not line:
        return
      if not self.quiet:
        self.output_stream.write(self.prefix + line.decode('utf-8'))
      self._output.write(line)

  def output(self):
    return self._output.getvalue()

def _checked_subprocess(quiet=False, *args, **kw):
  kw['stderr'] = subprocess.PIPE
  kw['stdout'] = subprocess.PIPE

  proc = subprocess.Popen(*args, **kw)
  stdout = TeeStream(quiet, proc.stdout, prefix='OUT: ')
  stderr = TeeStream(quiet, proc.stderr, prefix='ERR: ')
  stdout.start()
  stderr.start()
  status_code = proc.wait()
  stdout.join()
  stderr.join()
  if status_code != 0:
    logging.error(stdout.output())
    logging.error(stderr.output())
    raise ProcessException(status_code, stdout.output(), stderr.output())

  return stdout.output()

def shell_cmd(fmt, *args):
  cmd = _format_cmd(args, fmt)
  return _checked_subprocess(False, cmd, shell=True)

def shell_cmd_quiet(fmt, *args):
  cmd = _format_cmd(args, fmt)
  return _checked_subprocess(True, cmd, shell=True)

def _format_cmd(args, fmt):
  # print 'Running: %s: %s' % (fmt, args)
  if len(args) > 0:
    cmd = fmt % args
  else:
    cmd = fmt
  return cmd

def progress_bar(current, total, bar_length=20):
    fraction = current / total

    arrow = int(fraction * bar_length - 1) * '-' + '>'
    padding = int(bar_length - len(arrow)) * ' '

    ending = '\n' if current == total else '\r'

    print(f'Progress: [{arrow}{padding}] {int(fraction*100)}%', end=ending)