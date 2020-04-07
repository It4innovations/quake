from quake.client.base.task import Task, make_input


# TX[CPUS, Outputs]
#
# T1[2]
# |         T3[2]
# T2[3]     |
# |\        |
# | \      / \
# |  \    /   \
# |  T4[4,2]     T5[1]
# | (0)  (1)    /
# | /     \    /
# |/       \  /
#  T6[2]  T7[2,2]
#   \    (0)(1)
#    \   / /
#     T8[4]
from quake.server import Worker
from quake.server.state import State


def make_workers(count):
    return [Worker(i, "w_{}".format(i)) for i in range(count)]


def inp(task, output_id=0, layout="all_to_all"):
    return make_input(task, output_id, layout)


def make_plan1():
    t1 = Task(1, 1, 2, None, False, [])
    t2 = Task(2, 1, 3, None, False, [inp(t1)])
    t3 = Task(3, 1, 2, None, False, [])
    t4 = Task(4, 2, 4, None, False, [inp(t2), inp(t3)])
    t5 = Task(5, 1, 1, None, False, [inp(t3)])
    t6 = Task(6, 1, 2, None, False, [inp(t4), inp(t2), inp(t2)])
    t7 = Task(7, 2, 2, None, False, [inp(t4, 1), inp(t5)])
    t8 = Task(8, 1, 4, None, False, [inp(t6), inp(t7, 1), inp(t7)])
    tasks = [t1, t2, t3, t4, t5, t6, t7, t8]
    return [t.to_dict() for t in tasks]


def test_plan():
    plan1 = make_plan1()
    state = State(make_workers(4))
    has_ready_tasks = state.add_tasks(plan1)
    assert has_ready_tasks
    assert state.tasks.get(8).b_level == 1
    assert state.tasks.get(7).b_level == 2
    assert state.tasks.get(6).b_level == 2
    assert state.tasks.get(5).b_level == 3
    assert state.tasks.get(4).b_level == 3
    assert state.tasks.get(3).b_level == 4
    assert state.tasks.get(2).b_level == 4
    assert state.tasks.get(1).b_level == 5

    s = list(state.schedule())
    s.sort(key=lambda x: x[0].task_id)
    s1, s3 = s
    assert s1[0].task_id == 1
    assert len(s1[1]) == 2
    assert s3[0].task_id == 3
    assert len(s3[1]) == 2
    assert len(set(s3[1] + s1[1])) == 4

    state.on_task_finished(s[0][0], s[0][1], [[100, 100]])
    state.on_task_finished(s[1][0], s[1][1], [[20, 20]])

    s = list(state.schedule())
    s.sort(key=lambda x: x[0].task_id)
    s2, s5 = s
    assert s2[0].task_id == 2
    assert len(s2[1]) == 3
    assert len(set(s2[1] + s1[1])) == 3
    assert s5[0].task_id == 5
    assert len(s5[1]) == 1
    assert len(set(s3[1] + s5[1])) == 2
    assert len(set(s2[1] + s5[1])) == 4


def test_greedy_match():

    # T4 inputs
    #  t1   t1   t1 |  t2 |  t3   t3
    # ------------------------------
    #  100   0    0 | 200 |   1    0
    #    0 100    0 | 200 |   0    2
    #    0   0  100 | 200 |   1    0

    # placements
    #    |  t1   t1   t1 | t2  | t3  t3
    # ---------------------------------
    # w0 |   0    0    0 |   0 |  0   0
    # w1 |   0    0    0 |   0 |  1   0
    # w2 |   0    0    0 | 200 |  1   2
    # w3 | 100    0    0 | 200 |  0   0

    t1 = Task(1, 1, 3, None, False, [])
    t2 = Task(2, 1, 1, None, False, [])
    t3 = Task(3, 1, 2, None, False, [])
    t4 = Task(4, 0, 3, None, False, [inp(t1, 0, "scatter"), inp(t2, 0, "all_to_all"), inp(t3, 0, "scatter")])
    workers = make_workers(4)
    state = State(workers)
    state.add_tasks([t.to_dict() for t in [t1, t2, t3, t4]])

    t1, t2, t3, t4 = [state.tasks[i] for i in range(1, 5)]
    _, w1, w2, w3 = workers

    state._fake_placement(t1, [[{w1}, {}, {}]], [[100, 101, 102]])
    state._fake_placement(t2, [[{w2, w3}]], [[200]])
    state._fake_placement(t3, [[{w1, w2}, {w2}]], [[1, 2]])

    s = list(state.schedule())
    assert(len(s) == 1)
    assert(s[0][1] == [w2, w3, w1])