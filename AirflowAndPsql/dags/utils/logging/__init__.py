def get_stdout_stderr_logger():
    import logging

    from sys import stdout

    log_ = logging.getLogger()
    log_.setLevel(logging.INFO)

    log_handler_info = logging.StreamHandler(stdout)
    log_handler_info.setLevel(logging.INFO)
    log_handler_info.addFilter(lambda record: record.levelno <= logging.INFO)

    log_handler_warning = logging.StreamHandler()
    log_handler_warning.setLevel(logging.WARNING)
    log_.addHandler(log_handler_info)
    log_.addHandler(log_handler_warning)

    return log_


log = get_stdout_stderr_logger()
