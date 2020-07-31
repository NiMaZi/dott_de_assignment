import sys
import random
import grequests
from time import time

QR_CODE_LIST = []
URL_PREFIX = "http://peaceful-tide-284813.ew.r.appspot.com/vehicles/"

# Python < 3.6 workaround
def choices(seq, k):
    return [random.choice(seq) for _ in range(k)]

random.choices = choices
# Please delete/comment if Python >= 3.6 is available

def run(num_requests = 5000):
    sample_qr_codes = random.choices(QR_CODE_LIST, k = num_requests)
    urls = [URL_PREFIX + qr_code for qr_code in sample_qr_codes]

    rs = (grequests.get(u) for u in urls)
    s = time()
    res = grequests.map(rs)
    e = time()
    valid_res = [r for r in res if r]

    print("Got {} valid responses in {} seconds.")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        run(int(sys.argv[1]))
    else:
        run()