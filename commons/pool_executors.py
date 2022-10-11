import concurrent.futures
import logging
import sys

from tqdm import tqdm

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))


def process_parallel_run(coreFun, elements, *args, max_workers=3, ignore_results=True):
    """
    Call this to run a function in multi-processes.

    :param ignore_results: pass this to false to get the result of the processes.
    :param coreFun: function that takes 1 element from elements as parameter and the *args
    :param elements: array of elements to pass to coreFun one by one.
    :param args: rest of parameters of coreFun.
    :param max_workers: maximum number of processes in parallel.
                        If it's 0, their will be as many processes as CPUs.
                        If it's 1, their will ne no pool and no progress printing.
    :return: array of results of coreFun executions.
    """
    log.info('process_parallel_run max_workers = {0}'.format(str(max_workers)))
    if max_workers == 1:
        return [coreFun(l, *args) for l in elements]
    results = []
    with concurrent.futures.ProcessPoolExecutor(
            max_workers=max_workers) as executor:  # ThreadPoolExecutor or ProcessPoolExecutor
        try:
            futures = {executor.submit(coreFun, l, *args): l for l in elements}
            for future in concurrent.futures.as_completed(futures):
                url = futures[future]
                if not ignore_results:
                    try:
                        data = future.result()
                        results.append(data)

                    except Exception as exc:
                        log.error('%r generated an exception: %s' % (url, exc))
                        raise
                # else:
                #     print('%r page is %d bytes' % (url, len(data)))
                kwargs = {
                    'total': len(futures),
                    'unit': 'files',
                    'unit_scale': True,
                    'leave': False
                }
                # Print out the progress as tasks complete
                for f in tqdm(concurrent.futures.as_completed(futures), **kwargs):
                    pass
        except Exception as e:
            log.error(e)
            executor.shutdown()
            raise
    return results


# @see iFixR
def thread_parallel_run(coreFun, elements, *args, max_workers=4, ignore_results=True):
    log.info('process_parallel_run max_workers = {0}'.format(str(max_workers)))
    if max_workers == 1:
        return [coreFun(l, *args) for l in elements]
    results = []
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers) as executor:  # ThreadPoolExecutor or ProcessPoolExecutor
        try:
            futures = {executor.submit(coreFun, l, *args): l for l in elements}
            for future in concurrent.futures.as_completed(futures):
                url = futures[future]
                if not ignore_results:
                    try:
                        data = future.result()
                        results.append(data)

                    except Exception as exc:
                        log.error('%r generated an exception: %s' % (url, exc))
                        raise
                # else:
                #     print('%r page is %d bytes' % (url, len(data)))
                kwargs = {
                    'total': len(futures),
                    'unit': 'files',
                    'unit_scale': True,
                    'leave': False
                }
                # Print out the progress as tasks complete
                for f in tqdm(concurrent.futures.as_completed(futures), **kwargs):
                    pass
        except Exception as e:
            log.error(e)
            executor.shutdown()
            raise
    return results
