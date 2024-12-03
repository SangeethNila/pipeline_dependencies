from antlr4 import FileStream, CommonTokenStream, InputStream
from antlr_gen import JavaScriptLexer
from antlr_gen import JavaScriptParser

JSL = JavaScriptLexer.JavaScriptLexer
JSP = JavaScriptParser.JavaScriptParser

def parse_javascript_file(file_path):
    input_stream = FileStream(file_path)

    lexer = JSL(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = JSP(token_stream)

    tree = parser.program()

    return tree 

def parse_javascript_string(js_code):
    input_stream = InputStream(js_code)

    lexer = JSL(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = JSP(token_stream)

    tree = parser.program()

    return tree 

def parse_javascript_expression_string(js_code):
    input_stream = InputStream(js_code)

    lexer = JSL(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = JSP(token_stream)

    tree = parser.expressionStatement()

    return tree 
