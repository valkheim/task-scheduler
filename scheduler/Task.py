from enum import IntEnum


class Priorities(IntEnum):
    """ Task priorities """

    LOW = 10
    MEDIUM = 5
    HIGH = 0


class Task:
    def __init__(self, job, priority=Priorities.MEDIUM):
        """ Task constructor
        :param job: task job (callable)
        :param priority: task priority in scheduler queue
        """
        self.job = job
        self.priority = priority

    def __lt__(self, other):
        """ Comparison between Tasks so PriorityQueue can sort them """
        return self.priority < other.priority
