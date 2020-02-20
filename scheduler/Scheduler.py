import threading
import time
from enum import Enum
from queue import Empty, PriorityQueue

from .Singleton import Singleton
from .Task import Task


class SchedulerPolicies(Enum):
    """ Scheduler will process task accordingly to the policy defined by the
    validator. """

    RETRY = "retry"
    DROP = "drop"


def _default_validator(job) -> SchedulerPolicies:
    """ Default task validator. A task validator will start the task and decide
    on a scheduler policy.

    :param job: the task job
    :return: scheduler policy
    """
    try:
        job()
    except Exception:
        return SchedulerPolicies.DROP
    return SchedulerPolicies.RETRY


class Scheduler(metaclass=Singleton):
    """ Scheduler will process task on a regular basis (`interval`). """

    tasks = PriorityQueue()

    def __init__(self, validator=_default_validator, interval=1):
        """
        :param validator: task validation function
        :param interval: interval between two task processing
        """
        self._timer = None
        self.interval = interval
        self.is_running = False
        self.next_call = time.time()
        self.validator = validator

    def start(self):
        """ Start working on the queued tasks """
        if not self.is_running:
            self.next_call += self.interval
            self._timer = threading.Timer(self.next_call - time.time(), self._get)
            self._timer.start()
            self.is_running = True

    def _get(self):
        """ Thread wrapper for the runner """
        self.is_running = False
        self.start()
        try:
            self.run()
        except Empty:
            pass

    def _flush(self):
        """ Empty the tasks queue """
        while not self.tasks.empty():
            try:
                self.tasks.get(False)
            except Empty:
                continue
            self.tasks.task_done()

    def stop(self):
        """ Stop working on the queue """
        self._timer.cancel()
        self.is_running = False
        self._flush()

    def add(self, task: Task):
        """ Add a `Task` to tasks the queue """
        self.tasks.put((task))

    def run(self):
        """ Pop a task and run validator on it """
        task = self.tasks.get_nowait()
        policy = self.validator(task.job)
        if policy == SchedulerPolicies.RETRY:
            self.add(task)
        elif policy == SchedulerPolicies.DROP:
            pass
        self.tasks.task_done()

    def __del__(self):
        """ Stop the scheduler on deletion """
        self.stop()
