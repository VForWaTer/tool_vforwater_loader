from logger import logger

# define a handler for whiteboxgis tools verbose output
def whitebox_log_handler(msg: str):
    # following https://www.whiteboxgeo.com/manual/wbt_book/python_scripting/tool_output.html
    # we can ignore all lines that contain a '%' sign as that is the progress bar
    if '%' in msg or msg.startswith('*'):
        return
    elif 'error' in msg.lower():
        logger.error(f"WhiteboxTools Errored: {msg}")
    else:
        logger.debug(f"WhiteboxTools info: {msg}")
