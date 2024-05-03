from evagram_input.input_tool import input_tool


def input_data(**kwargs):
    session = input_tool.Session(**kwargs)
    session.input_data()
