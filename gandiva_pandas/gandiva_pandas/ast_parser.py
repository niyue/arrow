import ast
from pprint import pprint

class BinOpVisitor(ast.NodeVisitor):

    def visit_BinOp(self, node):
        print('Node type: BinOp\nFields: ', node._fields)
        self.generic_visit(node)

    def visit_Name(self,node):
        print('Node type: Name\nFields: ', node._fields)
        ast.NodeVisitor.generic_visit(self, node)


visitor = BinOpVisitor()
tree = ast.parse('a & b', mode='eval')
pprint(ast.dump(tree))
visitor.visit(tree)