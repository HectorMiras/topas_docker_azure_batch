import io

DEFAULT_ENCODING = "utf-8"

def query_yes_no(question: str, default: str = "yes") -> str:
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError(f"Invalid default answer: '{default}'")

    choice = default

    while 1:
        user_input = input(question + prompt).lower()
        if not user_input:
            break
        try:
            choice = valid[user_input[0]]
            break
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")

    return choice

def _read_stream_as_string(stream, encoding) -> str:
    """
    Read stream as string

    :param stream: input stream generator
    :param str encoding: The encoding of the file. The default is utf-8.
    :return: The file content.
    """

    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = DEFAULT_ENCODING
        return output.getvalue().decode(encoding)
    finally:
        output.close()