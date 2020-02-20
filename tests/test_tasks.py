from scheduler.Task import Priorities, Task


job = lambda: print("hello hackers!")

def test_instance():
    task = Task(job)

    assert task.job == job
    assert task.priority == Priorities.MEDIUM

def test_comparison():
    t1 = Task(job, 1)
    t2 = Task(job, 2)
    assert t1 < t2
