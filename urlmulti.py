# -*- encoding: UTF-8 -*-
'''Web crawling with multiprocessing
    
    List of functions:
        open_urls
        update
'''

# built-in modules
import multiprocessing as mp
import urllib2
import time

#==============================================================================
def open_urls(urls, processes=0, retries=0, silence=True):
    '''Open urls using multiple proccesses
    
    Args:
        urls (list(str)): urls to all webpages
        processes (int): number of worker processes
        retries=0 (int): number of times to retry a webpage before giving up
        silence=True (bool): if False, the progress will be written to stdout
    Returns:
        {url: pg_txt, url: pg_txt, url: pg_txt, ...}
        url (str): webpage url
        pg_txt (str|None): the webpage text of the url request, or None when
            the url request fails
    Note:
        The length of the returned dict will always be equal to the length of
        the urls argument list.
    '''
    
    # multi-process safe queue for results
    manager = mp.Manager()
    results = manager.Queue()
    
    # multi-process safe counter for tracking progress
    cnt_manager = mp.Manager()
    count = cnt_manager.Value('i', 0)
    count_lock = cnt_manager.Lock()
    
    # Create worker pool and start all jobs
    processes = int(processes)
    if processes < 1: processes = mp.cpu_count()
    worker_pool = mp.Pool(processes=processes)
    for url in urls:
        worker_pool.apply_async(
            __urlopen_async, args=( url, results, count, count_lock,
                                    silence, retries
                                  )
        )
    # END for
    worker_pool.close()
    
    # Start an updater process
    if not silence:
        updater = mp.Process(
            target=update, args=(len(urls), count, count_lock, 10)
        )
        updater.start()
    # END if
    
    # Wait here for all work to complete
    worker_pool.join()
    
    # End the updater process
    if not silence:
        updater.terminate()
        print 'All parallel work completed'
    # END if
    
    # Dump results Queue into a dict
    out_dict = {}
    while not results.empty():
        item = results.get()
        out_dict[item[0]] = item[1]
    
    return out_dict
# END urlopen_async

def __urlopen_async(url, results, count, count_lock, silence=True, retry=0):
    '''Worker function: Retrieves a single webpage'''
    try:
        # download page text
        f = urllib2.urlopen(url)
        pg_txt = f.read()
        f.close()
    except urllib2.URLError:
        if retry > 0:
            if not silence: print 'Retrying download of {0}...'.format(url)
            __urlopen_async(
                url, results, count, count_lock, silence, retry-1
            )
        else:
            if not silence: print 'page skipped: {}'.format(url)
            pg_txt = None
        # END if
    except Exception as err:
        print repr(err)
        raise err
    # END try
    
    # add the page text to the results queue
    results.put( (url, pg_txt) )
    with count_lock: count.value += 1
# END __urlopen_async

#==============================================================================
def update(total, count, count_lock, interval=2):
    t_0 = time.time()
    while True:
        with count_lock: n = count.value
        secs = time.time() - t_0
        hrs = int(secs/60/60)
        secs -= 60*60*hrs
        mins = int(secs/60)
        secs -= 60*mins
        
        x = 100.0*float(n)/total
        print (
            '({0:02d}:{1:02d}:{2:06.3f}) '.format(hrs, mins, secs) +
            '{0:02.0f}% complete ({1}/{2})'.format(x, n, total)
        )
        time.sleep(interval)
    # END while
# END update