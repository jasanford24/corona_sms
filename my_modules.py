#                                 - IMPORTS -                                 #
###############################################################################
# - Timer()
from time import perf_counter
from contextlib import ContextDecorator
from dataclasses import dataclass, field
from typing import Any, Callable, ClassVar, Dict, Optional


###############################################################################
# - create_driver()
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sys import platform


###############################################################################
# https://realpython.com/python-timer/

class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""


@dataclass
class Timer(ContextDecorator):
    """Time your code using a class, context manager, or decorator"""

    timers: ClassVar[Dict[str, float]] = dict()
    name: Optional[str] = None
    text: str = "Elapsed time: {:0.4f} seconds"
    logger: Optional[Callable[[str], None]] = print
    _start_time: Optional[float] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialization: add timer to dict of timers"""
        if self.name:
            self.timers.setdefault(self.name, 0)

    def start(self) -> None:
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = perf_counter()

    def stop(self) -> float:
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        # Calculate elapsed time
        elapsed_time = perf_counter() - self._start_time
        self._start_time = None

        # Report elapsed time
        if self.logger:
            self.logger(self.text.format(elapsed_time))
        if self.name:
            self.timers[self.name] += elapsed_time

        return elapsed_time

    def __enter__(self) -> "Timer":
        """Start a new timer as a context manager"""
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        """Stop the context manager timer"""
        self.stop()

###############################################################################


if platform == 'darwin':  # For collection on my Macbook
    BROWSER = '/Applications/Brave Browser.app/Contents/MacOS/Brave Browser'
    DRIVER = '/Users/noumenari/Documents/Python Projects/chromedriver'
elif platform == 'linux':  # For my Raspberry Pi
    BROWSER = '/usr/bin/chromium-browser'
    DRIVER = 'chromedriver'


def create_driver():
    options = Options()
    options.headless = True
    options.add_argument('--incognito')
    options.binary_location = BROWSER
    driver_path = DRIVER
    return webdriver.Chrome(options=options, executable_path=driver_path)

###############################################################################
