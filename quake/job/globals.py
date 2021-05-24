
_is_inside_job = False


def is_inside_job():
    return _is_inside_job


def set_job_flag():
    global _is_inside_job
    _is_inside_job = True
