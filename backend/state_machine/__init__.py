################################################################################
# !!! IMPORTANT !!!
#  This __init__.py allows to load the relevant classes from the State Machine.
#  By importing this file, we leverage "globals" and "getattr" to dynamically
#  execute the Step Function's inner Lambda Functions classes.
################################################################################

# States Entrypoints
from state_machine.states.convert_video_to_images import ConvertVideoToImages  # noqa
from state_machine.states.process_images import ProcessImages  # noqa
from state_machine.states.success import Success  # noqa
from state_machine.states.failure import Failure  # noqa
