from random import shuffle
from src.defs import postgres as p

from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns
from src.utils import user_info
from src.utils import static 

DELIMITER = ",_,"
FIRST_SESSION_FAVE_PCT = .9
FAVE_PCT= .55

