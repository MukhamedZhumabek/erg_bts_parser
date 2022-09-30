import sys
import logging
from job.bankrupt_lists import BankruptListJob

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("No target specified")
        exit(1)

    target = sys.argv[1]

    if target == "bankrupt":
        BankruptListJob.run()
