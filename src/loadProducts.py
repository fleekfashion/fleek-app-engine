from random import shuffle
from src.defs import postgres as p

from src.utils.psycop_utils import cur_execute, get_labeled_values, get_columns
from src.utils import user_info
from src.utils import static 

import sqlalchemy as s
from sqlalchemy.sql.expression import literal
from sqlalchemy.sql import text
import src.utils.query as qutils
from src.utils.sqlalchemy_utils import row_to_dict, session_scope 


DELIMITER = ",_,"
FIRST_SESSION_FAVE_PCT = .9
FAVE_PCT= .55


