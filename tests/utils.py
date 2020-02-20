from scheduler.Task import Priorities, Task


def generate_task():
    return Task(lambda: print("hello hackers!"), Priorities.MEDIUM)
