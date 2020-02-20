from time import sleep

from mock import Mock, patch

from scheduler.Scheduler import Scheduler, SchedulerPolicies
from scheduler.Task import Priorities, Task

from .utils import generate_task


def raise_task():
    print("raise_task")
    [][42]


def print_task():
    print("print_task")


def test_instance():
    assert isinstance(Scheduler(), Scheduler)
    assert id(Scheduler()) == id(Scheduler())


def test_add_task():
    scheduler = Scheduler()
    scheduler.start()

    assert scheduler.tasks.qsize() == 0
    scheduler.add(generate_task())
    assert scheduler.tasks.qsize() == 1

    scheduler.stop()


def test_runner():
    scheduler = Scheduler(interval=1)

    runner = scheduler.run
    scheduler.run = Mock()

    scheduler.start()
    sleep(2)
    scheduler.stop()

    assert 1 <= scheduler.run.call_count <= 2

    scheduler.run = runner


@patch("tests.test_scheduler.raise_task")
@patch("tests.test_scheduler.print_task")
def test_default_validator(mock_print, mock_raise):
    """ Test the default task validator

    Default validator (_default_validator) behaviour
    - task raises: retry task
    - task finishes: continue to next task
    """
    scheduler = Scheduler(interval=1)

    raise_task = Task(mock_raise, Priorities.HIGH)
    print_task = Task(mock_print, Priorities.MEDIUM)

    scheduler.add(print_task)
    scheduler.add(raise_task)

    scheduler.start()
    sleep(3)
    scheduler.stop()

    assert 2 <= mock_raise.call_count <= 3
    assert mock_print.call_count == 0


@patch("tests.test_scheduler.print_task")
def test_custom_validator(mock_print):
    def test_validator(job):
        job()
        return SchedulerPolicies.RETRY

    scheduler = Scheduler(validator=test_validator, interval=1)
    print_task = Task(mock_print, Priorities.MEDIUM)

    scheduler.add(print_task)

    scheduler.start()
    sleep(3)
    scheduler.stop()

    assert 2 <= mock_print.call_count <= 3
