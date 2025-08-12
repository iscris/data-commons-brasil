import logging

from datatools.main import main

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s] [%(name)s] [%(asctime)s]: %(message)s"
)

main()
