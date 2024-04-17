import ast
import pyarrow as pa
import pyarrow.gandiva as gandiva
import pandas as pd


class LLVMGandivaVisitor(ast.NodeVisitor):
    cached_filters = {}
    df_table = None

    def __init__(self, df_table):
        self.table = df_table
        self.builder = gandiva.TreeExprBuilder()
        self.columns = {f.name: self.builder.make_field(f)
                        for f in self.table.schema}
        self.compare_ops = {
            "Gt": "greater_than",
            "Lt": "less_than",
        }
        self.bin_ops = {
            "BitAnd": self.builder.make_and,
            "BitOr": self.builder.make_or,
        }
    
    def visit_Module(self, node):
        return self.visit(node.body[0])
    
    def visit_BinOp(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)
        op_name = node.op.__class__.__name__
        # print(f"op name: " + op_name)
        gandiva_bin_op = self.bin_ops[op_name]
        return gandiva_bin_op([left, right])

    def visit_Compare(self, node):
        op = node.ops[0]
        op_name = op.__class__.__name__
        gandiva_comp_op = self.compare_ops[op_name]
        comparators = self.visit(node.comparators[0])
        left = self.visit(node.left)
        return self.builder.make_function(gandiva_comp_op,
                                          [left, comparators], pa.bool_())
        
    def visit_Constant(self, node):
        return self.builder.make_literal(node.n, pa.float64())

    def visit_Expr(self, node):
        return self.visit(node.value)
    
    def visit_Name(self, node):
        return self.columns[node.id]
    
    def generic_visit(self, node):
        return node

    def create_filter(self, llvm_mod, dump_ir=False):
        condition = self.builder.make_condition(llvm_mod)
        config = gandiva.Configuration(dump_ir=dump_ir)
        filter = gandiva.make_filter(self.table.schema, condition, config)
        if dump_ir:
            print(filter.llvm_ir)
        return filter 
    
    def evaluate_filter(self, filter):
        result = filter.evaluate(self.table.to_batches()[0],
                                  pa.default_memory_pool())    
        arr = result.to_array()
        pd_result = arr.to_numpy()
        return pd_result
    

    @staticmethod
    def gandiva_query(df_table, query, dump_ir=False):
        llvm_gandiva_visitor = LLVMGandivaVisitor(df_table)

        if query not in llvm_gandiva_visitor.cached_filters:
            mod_f = ast.parse(query)
            llvm_mod = llvm_gandiva_visitor.visit(mod_f)
            # print(llvm_mod)
            filter = llvm_gandiva_visitor.create_filter(llvm_mod, dump_ir)
            llvm_gandiva_visitor.cached_filters[query] = filter
        else:
            filter = llvm_gandiva_visitor.cached_filters[query]

        results = llvm_gandiva_visitor.evaluate_filter(filter)
        return results

@pd.api.extensions.register_dataframe_accessor("gandiva")
class GandivaAcessor:
    def __init__(self, pandas_df):
        self.pandas_df = pandas_df
        self.df_table = pa.Table.from_pandas(pandas_df)

    def query(self, query, dump_ir=False):
         return LLVMGandivaVisitor.gandiva_query(self.df_table, query, dump_ir)