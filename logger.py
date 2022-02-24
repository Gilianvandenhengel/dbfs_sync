#%%
import logging
import textwrap
import inspect

class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        message = record.msg
        record.msg = ''
        header = super().format(record)
        msg= textwrap.indent(inspect.cleandoc(message), ' ' * len(header)).lstrip()
        # msg= textwrap.dedent(message).lstrip()
        record.msg = message
        return (header + msg).lstrip()

formatter = MultiLineFormatter(
    fmt='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

ch = logging.StreamHandler()
ch.setFormatter(formatter)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(ch)

